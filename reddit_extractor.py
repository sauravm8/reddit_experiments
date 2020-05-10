import logging
import config
from os.path import isfile
import praw
import pandas as pd


# Get credentials from DEFAULT instance in praw.ini
reddit = praw.Reddit()

class SubredditScraper:

    def __init__(self, sub, sort, limit):
        self.sub = sub
        self.sort = sort
        self.limit = limit
        logging.info("Subreddit scrapper created with values sub-->{}, sort-->{}, limit-->{}".format(sub, sort, limit))


    def set_sort(self):
        if self.sort == 'new':
            return self.sort, reddit.subreddit(self.sub)
        elif self.sort == 'top':
            return self.sort, reddit.subreddit(self.sub)
        elif self.sort == 'hot':
            return self.sort, reddit.subreddit(self.sub)
        else:
            self.sort = 'hot'
            logging.info('Sort method was not recognized, defaulting to hot.')
            return self.sort, reddit.subreddit(self.sub)

    def get_posts(self):

        # Creating a dataframe to store all the data that we are extracting from reddit
        reddit_df = pd.DataFrame(columns=[ "Title", "SelfText", "Id", "NumComments", "Score", "Ups", "Downs"])
        logging.info("Created dataframe to store all the values that we are gonna extract")

        # Attempt to specify a sorting method.
        sort, subreddit = self.set_sort()

        logging.info("Adding the posts into the dataframe")

        # Adding each post into the dataframe
        for post in subreddit.stream.submissions():
            reddit_df = reddit_df.append({'Title': post.title, 'SelfText': post.selftext,  'Id': post.id,
                                           "NumComments": post.num_comments, "Score": post.score,
                                          "Ups": post.ups, "Downs": post.downs}, ignore_index=True)
            logging.info("id-->{}".format(post.id))

            # Extracting all the comments
            """
            post.comments.replace_more(limit=None)
            comment_queue = post.comments[:]  # Seed with top-level
            while comment_queue:
                comment = comment_queue.pop(0)
                print(comment.body)
                comment_queue.extend(comment.replies)
            """

        # Dropping any duplicates, based on post id
        reddit_df = reddit_df.drop_duplicates("Id")
        logging.info("All the duplicates have been dropped")

        reddit_df.to_csv(config.OUT_FILE)
        logging.info("Written the output to file {}".format(config.OUT_FILE))

if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')
    logging.info("Starting extraction")
    SubredditScraper(config.SUBREDDIT_TO_EXTRACT, sort=config.SORT, limit=config.LIMIT, ).get_posts()