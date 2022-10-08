import time
import instaloader
import pandas as pd
from dotenv import load_dotenv
import os
import logging
import json

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)
load_dotenv()


def followers_scrapper(username_scrape):
    try:
        instance = instaloader.Instaloader(save_metadata=False, compress_json=False, max_connection_attempts=5)
        instance.login(user=os.environ['INSTA_USERNAME_1'], passwd=os.environ['INSTA_PASS_1'])
        profile = instaloader.Profile.from_username(instance.context, username=username_scrape)
        follow_list = []
        count = 0
        # logger.info("Getting followers")
        for followee in profile.get_followers():
            follow_list.append(followee.username)
            file = open(username_scrape + "/followers.csv", "a+")
            file.write(follow_list[count])
            file.write("\n")
            file.close()
            count = count + 1
            if count % 1000 == 0:
                time.sleep(10)
                print("break over")
    except Exception as e:
        logger.warning(e)
    # print("Followers lost have been made\n")


def following_scrapper(username_scrape):
    try:
        instance = instaloader.Instaloader(save_metadata=False, compress_json=False, max_connection_attempts=5)
        instance.login(user=os.environ['INSTA_USERNAME_3'], passwd=os.environ['INSTA_PASS_3'])
        profile = instaloader.Profile.from_username(instance.context, username=username_scrape)
        following_list = []
        count = 0
        # logger.info("Getting followees")
        for followee in profile.get_followees():
            following_list.append(followee.username)
            file = open(username_scrape + "/followingss.csv", "a+")
            file.write(following_list[count])
            file.write("\n")
            file.close()
            count = count + 1
            if count % 1000 == 0:
                time.sleep(1000)
    except Exception as e:
        logger.warning(e)
    # print("Following list have been made\n")


def likes_and_comments_scrapper(username_scrape, instance=None):
    try:
        instance = instaloader.Instaloader(save_metadata=False, compress_json=False, max_connection_attempts=5)
        instance.login(user=os.environ['INSTA_USERNAME_2'], passwd=os.environ['INSTA_PASS_2'])
        profile = instaloader.Profile.from_username(instance.context, username=username_scrape)
        for post in profile.get_posts():
            # print(post)
            list_likers = []
            list_comment = []
            post_likes = post.get_likes()
            post_comments = post.get_comments()

            n = 0
            count = 0
            logger.info(f"Getting likes for {post_likes}")
            for like in post_likes:
                list_likers.append((like.username))
                likers_file = open(username_scrape + "/likes_post.csv", "a+")
                likers_file.write(list_likers[n])
                n += 1
                if count % 1000 == 0:
                    time.sleep(1000)
                likers_file.write("\n")
                likers_file.close()
            x = {f"Count of post_{post_likes}": n}
            y = str(json.dumps(x))
            count_comments = open(username_scrape + "/counts of likes in Post.json", "a+")
            count_comments.write(y)
            count_comments.write("\n")
            count_comments.close()
            n = 0
            count = 0
            logger.info(f"Getting comments for {post_comments}")
            for comment in post_comments:
                list_comment.append(comment.owner.username)
                comment_file = open(username_scrape + "/comment_post.csv", "a+")
                comment_file.write(list_comment[n])
                if count % 1000 == 0:
                    time.sleep(1000)
                n += 1
                comment_file.write("\n")
                comment_file.close()
            x = {f"Count of post_{post_comments}": n}
            y = str(json.dumps(x))
            count_comments = open(username_scrape + "/counts of comments in Post.json", "a+")
            count_comments.write(y)
            count_comments.write("\n")
            count_comments.close()
        logger.info("Removing Duplicates from files")
        df_likers = pd.read_csv(username_scrape + "/likes_post.csv", header=None)
        df_likers.rename(columns={0: "Username"}, inplace=True)
        df_likers = df_likers.drop_duplicates(subset="Username")
        df_likers.to_csv(username_scrape + "/likes_post.csv", index=False)
        df_commnets = pd.read_csv(username_scrape + "/comment_post.csv", header=None)
        df_commnets.rename(columns={0: "Username"}, inplace=True)
        df_commnets = df_commnets.drop_duplicates(subset="Username")
        df_commnets.to_csv(username_scrape + "/comment_post.csv", index=False)
    except Exception as e:
        logger.warning(e)


def insta_scrapper(username_scrape):
    """Scrapes all media, followers, followee, post likes & post comments"""
    logger.info("Inside Scrapper function")
    try:
        instance = instaloader.Instaloader(save_metadata=False, compress_json=False, max_connection_attempts=5)

        instance.login(user=os.environ['INSTA_USERNAME_2'], passwd=os.environ['INSTA_PASS_2'])
        profile = instaloader.Profile.from_username(instance.context, username=username_scrape)
        instance.download_profile(profile_name=username_scrape)

        logger.info("Getting general details")
        x = {"Username": profile.username,
             "UserId": profile.userid,
             "Number of posts": str(profile.mediacount),
             "Number of Follwers": str(profile.followers),
             "Number of Following": str(profile.followees),
             "Bio": profile.biography
             }
        y = str(json.dumps(x))
        # with open(f"{username_scrape}/general_details.json", "a", encoding="utf-8") as writer:  # for writing
        with open(username_scrape + "/general_details.json", "a", encoding="utf-8", ) as writer:
            writer.write(y)
        logger.info("DONE")

        # list of all the follwers and followings

        logger.info("Getting followers")
        followers_scrapper(username_scrape)
        logger.info("Done")
        #
        logger.info("Getting following")
        following_scrapper(username_scrape)
        logger.info("Done")

        # To collect the list of all the liker and commenters
        logger.info("Getting list likers and commnenters")
        likes_and_comments_scrapper(username_scrape)
        logger.info("list of all the likers and commenters have been made\n")
    except Exception as e:
        logger.warning(e)
        print(f"ERROR for", username_scrape)


# function to collect username for scraping the data
def scrape_followers(name):
    """Scrapes followers for a particular account"""
    logger.info("Scraping", name)
    instance = instaloader.Instaloader(save_metadata=False, compress_json=False, )
    instance.login(user=os.environ['INSTA_USERNAME_2'], passwd=os.environ['INSTA_PASS_2'])
    profile = instaloader.Profile.from_username(instance.context, username=name)

    follow_list = []
    count = 0
    logger.info("Getting followers")
    for followee in profile.get_followers():
        follow_list.append(followee.username)
        file = open("data/instagram/instagram_names.csv", "a+")
        file.write(follow_list[count])
        file.write("\n")
        file.close()
        count = count + 1
    return follow_list


if __name__ == '__main__':
    # logger.info("Getting followers of cristiano")
    # follower_list = scrape_followers("cristiano")  # calling the function to scrap data
    # logger.info("Done")
    follower_data = pd.read_csv("data/Instagram/data.csv")
    # print(Follower_data["username"][0])
    follower_list = list(follower_data["username"])
    count = 0
    for name in follower_list:
        logger.info("Scrapping "+ name)
        insta_scrapper(name)
        logger.info("Scraped "+ name)
        follower_data = follower_data.drop([count])
        follower_data.to_csv("data/Instagram/data.csv", index=False)
        count += 1

