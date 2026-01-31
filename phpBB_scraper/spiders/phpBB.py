import re

import scrapy
from bs4 import BeautifulSoup
from scrapy.http import Request
import os
from dotenv import load_dotenv

load_dotenv()
# TODO: Please provide values for the following variables
# Domains only, no urls
ALLOWED_DOMAINS = ["macserialjunkie.com"]
# Starting urls
START_URLS = [
    "https://macserialjunkie.com/forum/index.php",
    ## Add missing sub-forums
    "https://macserialjunkie.com/forum/viewforum.php?f=53",  # msj.keygens
    "https://macserialjunkie.com/forum/viewforum.php?f=52",  # msj.stw.graphics
    "https://macserialjunkie.com/forum/viewforum.php?f=27",  # msj.stw.videotutorials
    "https://macserialjunkie.com/forum/viewforum.php?f=28",  # msj.stw.webdev
    "https://macserialjunkie.com/forum/viewforum.php?f=25",  # cracking.workshop
    "https://macserialjunkie.com/forum/viewforum.php?f=34",  # msj.games.cracks
    "https://macserialjunkie.com/forum/viewforum.php?f=35",  # msj.games.serials
    "https://macserialjunkie.com/forum/viewforum.php?f=63",  # msj.games.ports
    "https://macserialjunkie.com/forum/viewforum.php?f=56",  # msj.audio.cracks
    "https://macserialjunkie.com/forum/viewforum.php?f=57",  # msj.audio.serials
    "https://macserialjunkie.com/forum/viewforum.php?f=59",  # msj.iOS.games
]
# Is login required? True or False.
FORM_LOGIN = True
# Login username
USERNAME = os.getenv("USERNAME") or "username"
# Login password
PASSWORD = os.getenv("PASSWORD") or "password"
# Login url
LOGIN_URL = "https://macserialjunkie.com/forum/ucp.php"


class PhpbbSpider(scrapy.Spider):
    name = "phpBB"
    allowed_domains = ALLOWED_DOMAINS
    start_urls = START_URLS
    form_login = FORM_LOGIN
    if form_login is True:
        username = USERNAME
        password = PASSWORD
        login_url = LOGIN_URL
        start_urls.insert(0, login_url)

    username_xpath = (
        '//p[contains(@class, "author")]//a[contains(@class, "username")]//text()'
    )
    post_count_xpath = '//dd[@class="profile-posts" or not(@class)]//a/text()'
    post_time_xpath = '//div[@class="postbody"]//time/@datetime|//div[@class="postbody"]//p[@class="author"]/text()[2]'
    post_text_xpath = '//div[@class="postbody"]//div[@class="content"]'

    def parse(self, response):
        if self.form_login:
            formxpath = '//*[contains(@action, "login")]'
            formdata = {"username": self.username, "password": self.password}
            form_request = scrapy.FormRequest.from_response(
                response,
                formdata=formdata,
                formxpath=formxpath,
                callback=self.after_login,
                dont_click=False,
            )
            yield form_request
        else:
            links = response.xpath('//a[@class="forumtitle"]/@href').extract()
            for link in links:
                yield scrapy.Request(response.urljoin(link), callback=self.parse_topics)

    def after_login(self, response):
        if b"authentication failed" in response.body:
            self.logger.error("Login failed.")
            return
        else:
            links = response.xpath('//a[@class="forumtitle"]/@href').extract()
            for link in links:
                yield scrapy.Request(response.urljoin(link), callback=self.parse_topics)

    def parse_topics(self, response):
        links = response.xpath('//a[@class="topictitle"]/@href').extract()
        for link in links:
            yield scrapy.Request(response.urljoin(link), callback=self.parse_posts)

        next_link = response.xpath(
            '//li[contains(@class, "next")]//a[@rel="next"]/@href'
        ).extract_first()
        if next_link:
            # print("next_link: ", next_link)
            yield scrapy.Request(
                response.urljoin(next_link), callback=self.parse_topics
            )

    def clean_quote(self, string):
        soup = BeautifulSoup(string, "lxml")
        block_quotes = soup.find_all("blockquote")
        for i, quote in enumerate(block_quotes):
            block_quotes[i] = "<quote-%s>=%s" % (str(i + 1), quote.get_text())
        return "".join(block_quotes).strip()

    def clean_text(self, string):
        tags = ["blockquote"]
        soup = BeautifulSoup(string, "lxml")
        for tag in tags:
            for i, item in enumerate(soup.find_all(tag)):
                item.replaceWith("<reply-%s>=" % str(i + 1))
        return re.sub(r" +", r" ", soup.get_text()).strip()

    def parse_posts(self, response):
        # Try the hidden input field first
        topic_id = response.xpath(
            '//input[@type="hidden" and @name="t"]/@value'
        ).extract_first()
        # Fallback to URL regex if hidden input isn't found
        if not topic_id:
            topic_id_match = re.search(r"[?&]t=(\d+)", response.url)
            if topic_id_match:
                topic_id = topic_id_match.group(1)

        # This ensures IDs, Dates, and Text stay synchronized
        posts = response.xpath(
            '//div[contains(@class, "post") and contains(@class, "has-profile")]'
        )

        for post in posts:
            # The div usually has id="p123456", we want 123456
            div_id = post.xpath("./@id").extract_first()
            post_id = div_id.replace("p", "") if div_id else None

            # Modern phpBB themes usually have a hidden span with data attributes
            poster_id = post.xpath(
                './/span[contains(@class, "postdetails")]/@data-poster-id'
            ).extract_first()

            # Fallback: Extract from the profile link (e.g., ...&u=5465)
            if not poster_id:
                profile_link = post.xpath(
                    './/dt[contains(@class, "has-profile-rank")]/a[contains(@href, "mode=viewprofile")]/@href'
                ).extract_first()
                if profile_link:
                    u_match = re.search(r"[?&]u=(\d+)", profile_link)
                    if u_match:
                        poster_id = u_match.group(1)

            # Priority 1: The 'datetime' attribute (ISO format)
            post_time = post.xpath(
                './/p[@class="author"]//time/@datetime'
            ).extract_first()

            # Priority 2: The visible text inside the time tag
            if not post_time:
                post_time = post.xpath(
                    './/p[@class="author"]//time/text()'
                ).extract_first()

            username = post.xpath(
                './/dt[contains(@class, "has-profile-rank")]//a[contains(@class, "username")]/text()'
            ).extract_first()
            post_count = post.xpath(
                './/dd[@class="profile-posts"]//a/text()'
            ).extract_first()

            content_html = post.xpath('.//div[@class="content"]').extract_first() or ""
            post_text = self.clean_text(content_html)
            quote_text = self.clean_quote(content_html)

            yield {
                "TopicID": topic_id,
                "PostID": post_id,
                "PosterID": poster_id,
                "Username": username,
                "PostCount": post_count,
                "PostTime": post_time,
                "PostText": post_text,
                "QuoteText": quote_text,
            }

        # Updated to use contains(@class, "next") because class is "arrow next"
        next_link = response.xpath(
            '//li[contains(@class, "next")]/a[@rel="next"]/@href'
        ).extract_first()

        # Fallback: just look for the rel="next" attribute directly
        if not next_link:
            next_link = response.xpath('//a[@rel="next"]/@href').extract_first()

        if next_link:
            # print("next_link: ", next_link)
            yield scrapy.Request(response.urljoin(next_link), callback=self.parse_posts)
