import time
import instaloader
import pandas as pd
from dotenv import load_dotenv
import os
import logging

logger = logging.getLogger(__name__)
load_dotenv()


def scrapper(username_scrape):
    logger.info("Inside Scrapper function")
    try:
        instance = instaloader.Instaloader(save_metadata=False, compress_json=False)

        instance.login(user=os.environ['INSTA_USERNAME_1'], passwd=os.environ['INSTA_PASS_1'])
        profile = instaloader.Profile.from_username(instance.context, username=username_scrape)
        instance.download_profile(profile_name=username_scrape, download_tagged=True)

        file = open(f"{username_scrape}/Genral_Details.txt", "a+", encoding="utf-8")

        file.write(f"User_Name: {profile.username}")
        file.write("\n")
        file.write((f"User_Id: {str(profile.userid)}"))
        file.write("\n")
        file.write(f"Number_ of_Post: {str(profile.mediacount)}")
        file.write("\n")
        file.write(f"Number_of_Followers: {str(profile.followers)}")
        file.write("\n")
        file.write(f"Number_of_Followings: {str(profile.followees)}")
        file.write("\n")
        file.write(profile.biography)
        file.write("/n")
        file.close()
        file.close()
        print("Gernal details text file has been made\n")

        # list of all the follwers and followingt
        follow_list = []
        count = 0
        logger.info("Getting followers")
        for followee in profile.get_followers():
            follow_list.append(followee.username)
            file = open(f"{username_scrape}/followers.csv", "a+")
            file.write(follow_list[count])
            file.write("\n")
            file.close()
            count = count + 1
            if count % 1000 == 0:
                time.sleep(10)
                print("break over")
        print("Followers lost have been made\n")
        following_list = []
        count = 0
        logger.info("Getting followees")
        for followee in profile.get_followees():
            following_list.append(followee.username)
            file = open(f"{username_scrape}/followingss.csv", "a+")
            file.write(following_list[count])
            file.write("\n")
            file.close()
            count = count + 1
            if count % 1000 == 0:
                time.sleep(1000)
        print("Following list have been made\n")
        # To collect the list of all the liker and commenters
        a = 1
        logger.info("Getting posts")
        for post in profile.get_posts():
            # print(post)
            list_likers = []
            list_comment = []
            post_likes = post.get_likes()
            post_comments = post.get_comments()

            n = 0
            logger.info(f"Getting likes for {post.title}")
            for like in post_likes:
                list_likers.append((like.username))
                file = open(f"{username_scrape}/likes_post.txt", "a+")
                file.write(list_likers[n])
                n += 1
                if count % 1000 == 0:
                    time.sleep(1000)
                file.write("\n")
            file.close()
            file = open(f"{username_scrape}/counts of like in Post.txt", "a+")
            file.write(f"Count of post_{a}: {n}")
            file.write("\n")
            file.close()
            n = 0
            for comment in post_comments:
                list_comment.append((comment.owner.username))
                file = open(f"{username_scrape}/comment_post.txt", "a+")
                file.write(list_comment[n])
                if count % 1000 == 0:
                    time.sleep(1000)
                n += 1
            file = open(f"{username_scrape}/counts of comments in Post.txt", "a+")
            file.write(f"Count of post_{a}: {n}")
            file.write("\n")
            file.close()
            a += 1

        # print("list of all the likers and commenters have been made\n")
    except Exception as e:
        print(e)
        print(f"ERROR for {username_scrape}")


# function to collect username for scraping the data
def data(name):
    instance = instaloader.Instaloader(save_metadata=False, compress_json=False, )
    instance.login(user=os.environ['INSTA_USERNAME_2'], passwd=os.environ['INSTA_PASS_2'])
    profile = instaloader.Profile.from_username(instance.context, username=name)

    follow_list = []
    count = 0
    logger.info("Getting followers")
    for followee in profile.get_followers():
        follow_list.append(followee.username)
        file = open("names.csv", "a+")
        file.write(follow_list[count])
        file.write("\n")
        file.close()
        count = count + 1


if __name__ == '__main__':
    try:
        data("cristiano")  # calling the function to scrap data
    except Exception as e:
        print(e)

    list = pd.read_csv("names.csv")
    # print(list["name"])
    for name in list["name"]:
        scrapper(name)
        print("done")
