import time
import instaloader
import pandas as pd

def scrapper(Username_scrap):
    try:
       instance = instaloader.Instaloader(save_metadata=False, compress_json=False,)
       YOUR_USERNAME = "tg306277"
       YOUR_PASSWORD = "qwerty54321"

       # Username_scrap = input("Give account's username you want to scrap: ")
       instance.login(user=YOUR_USERNAME, passwd=YOUR_PASSWORD)
       profile = instaloader.Profile.from_username(instance.context, username=Username_scrap)
       instance.download_profile(profile_name=Username_scrap, download_tagged=True, )

       file = open(f"{Username_scrap}/Genral_Details.txt", "a+", encoding="utf-8")

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
       for followee in profile.get_followers():
           follow_list.append(followee.username)
           file = open(f"{Username_scrap}/followers.csv", "a+")
           file.write(follow_list[count])
           file.write("\n")
           file.close()
           count = count + 1
           if count%1000 == 0:
               time.sleep(10)
               print("break over")
       print("Followers lost have been made\n")
       following_list = []
       count = 0
       for followee in profile.get_followees():
           following_list.append(followee.username)
           file = open(f"{Username_scrap}/followingss.csv", "a+")
           file.write(following_list[count])
           file.write("\n")
           file.close()
           count = count + 1
           if count%1000 == 0:
               time.sleep(1000)
       print("Following list have been made\n")
       # To collect the list of all the liker and commenters
       a = 1
       for post in profile.get_posts():
           # print(post)
           list_likers = []
           list_comment = []
           post_likes = post.get_likes()
           post_comments = post.get_comments()

           n = 0
           for likee in post_likes:
               list_likers.append((likee.username))
               file = open(f"{Username_scrap}/likes_post.txt", "a+")
               file.write(list_likers[n])
               n += 1
               if count % 1000 == 0:
                   time.sleep(1000)
               file.write("\n")
           file.close()
           file = open(f"{Username_scrap}/counts of like in Post.txt", "a+")
           file.write(f"Count of post_{a}: {n}")
           file.write("\n")
           file.close()
           n = 0
           for comment in post_comments:
               list_comment.append((comment.owner.username))
               file = open(f"{Username_scrap}/comment_post.txt", "a+")
               file.write(list_comment[n])
               if count % 1000 == 0:
                   time.sleep(1000)
               n += 1
           file = open(f"{Username_scrap}/counts of comments in Post.txt", "a+")
           file.write(f"Count of post_{a}: {n}")
           file.write("\n")
           file.close()
           a += 1

       # print("list of all the likers and commenters have been made\n")
    except Exception as e:
        print(e)
        print(f"ERROR for {Username_scrap}")

#Cfunction to collect username for scraping the data
def data(name):
    instance = instaloader.Instaloader(save_metadata=False, compress_json=False, )
    YOUR_USERNAME = "tanishsharma869"
    YOUR_PASSWORD = "qwerty54321"

    Username_scrap = name
    instance.login(user=YOUR_USERNAME, passwd=YOUR_PASSWORD)
    profile = instaloader.Profile.from_username(instance.context, username=Username_scrap)

    follow_list = []
    count = 0
    for followee in profile.get_followers():
        follow_list.append(followee.username)
        file = open("names.csv", "a+")
        file.write(follow_list[count])
        file.write("\n")
        file.close()
        count = count + 1

try:
    data("cristiano") #calling the function to scrap data
except Exception as e:
    print(e)


list = pd.read_csv("names.csv")
# print(list["name"])
for name in list["name"]:
    Username_scrap = name
    scrapper((Username_scrap))
    print("done")
