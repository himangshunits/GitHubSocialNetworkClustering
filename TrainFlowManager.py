import sys
import pandas as pd
import ConfigurationManager as cfg
import datetime
from CustomExceptions import *
import graphlab as gl
#import dateparser
import numpy as np



class TrainFlowManager:
    'This class collects the data from database, synthesizes the data in the form the recommendation engines can use and then make the models for reco.'

    def __init__(self):
        # TODO : Correct the datatypes here!
        self.user_data = pd.read_csv(cfg.user_data_filename, sep=',', encoding='utf-8')
        self.user_data.drop(self.user_data.columns[[0]], axis=1, inplace=True)
        #print self.user_data.dtypes
        self.user_orig_data = pd.read_csv(cfg.user_orig_data_filename, sep=',', encoding='utf-8')
        self.user_orig_data.drop(self.user_orig_data.columns[[0]], axis=1, inplace=True)
        #print self.user_orig_data.dtypes

        self.repo_data = pd.read_csv(cfg.repo_data_filename,
                                     sep=',', encoding='utf-8', dtype={"repo_id":"int64"	,"owner_id":"int64"	,
                                                                                               "is_private":"bool"	,"is_forked":"bool"	,"cont_count":"int64"	,
                                                                                               "language":"string"	,"days_from_creation":"int64"	,"days_from_updation":"int64"	,
                                                                                               "days_from_push":"int64"	,"size":"int64"	,"watcher_count":"int64"	,
                                                                                               "stargazer_count":"int64"	,"has_wiki":"bool"	,"fork_count":"int64"	,
                                                                                               "open_issues":"int64"	,"sub_count":"int64"	,"readme":"string"	,"description":"string"})


        self.repo_data.drop(self.repo_data.columns[[0]], axis=1, inplace=True)
        # Replace NaNs
        self.repo_data['language'].fillna(' ', inplace=True)
        self.repo_data['readme'].fillna(' ', inplace=True)
        self.repo_data['description'].fillna(' ', inplace=True)


        #print self.repo_data.dtypes
        # Repo Data is a must for converting the dTypes. Do it above! Or cast all of them as object?


        self.repo_orig_data = pd.read_csv(cfg.repo_orig_data_filename, sep=',', encoding='utf-8')
        self.repo_orig_data.drop(self.repo_orig_data.columns[[0]], axis=1, inplace=True)
        #print self.repo_orig_data.dtypes

        # Replace NaNs
        self.repo_orig_data['language'].fillna(' ', inplace=True)
        self.repo_orig_data['readme'].fillna(' ', inplace=True)
        self.repo_orig_data['description'].fillna(' ', inplace=True)




        # Create a Data frame to keep the aggregated values per user for all his repos.
        self.user_repo_aggregated = pd.DataFrame(columns=["user_id", "repo_count", "is_forked", "has_wiki", "cont_count", "forks_count", "subscribers_count", "language"])


        # TODO : DropNA?
        # Print the shapes.
        print "user_data.shape" + str(self.user_data.shape)
        print "user_orig_data.shape" + str(self.user_orig_data.shape)
        print "repo_data.shape" + str(self.repo_data.shape)
        print "repo_orig_data.shape" + str(self.repo_orig_data.shape)
        self.create_aggregation()



    # this API will give the internal stuff of this class
    def get_data_structures(self):
        return self.user_orig_data, self.repo_orig_data, self.user_data, self.repo_data

    def __none_checker_int(self, input):
        return input if input is not None else 0

    def __none_checker_string(self, input):
        return input if input is not None else ""

    def __get_date_diff(self, input_date):
        curr_date = datetime.datetime.today()
        if input_date is None:
            return curr_date
        #print "The input type of the date is =" + str(type(input_date))
        #parsed_date = dateparser.parse(str(input_date))
        parsed_date = input_date.to_pydatetime()
        diff_in_days = (curr_date - parsed_date).days
        return diff_in_days






    def __map_bool_to_int(self, input_bool):
        return 1 if input_bool == True else -1



    def aggregate_boolean_series(self, bool_list):
        counts = bool_list.value_counts(sort=True, ascending=False, dropna=True)
        numerator = 0
        denominator = 0
        for index, value in counts.iteritems():
            numerator += self.__map_bool_to_int(index) * value
            denominator += value
        try:
            result = float(numerator)/denominator
        except Exception as e:
            result = 0
            print "Empty Series detected in Bool Aggregation = " + str(bool_list) + str(e)
            raise BadOrEmptySeriesException
        return result

    def aggregate_cat_series(self, cat_list):
        counts = cat_list.value_counts(sort=True, ascending=False, dropna=True)
        # Find the first non blank string
        i = 0
        result = ""
        while result.strip() == "":
            if i >= counts.size:
                break
            result = counts.index[i]
            i += 1
        return result

    def aggregate_numerical_series(self, num_list):
        if cfg.numerical_aggregation_function == "mean":
            result = num_list.mean()
            return result
        elif cfg.numerical_aggregation_function == "median":
            result = num_list.median()
            return result
        else:
            print "Unknown aggregation function for numerical series!"
            raise Exception


    def create_aggregation(self):
        for index, row in self.user_data.iterrows():
            print "Inside Aggregation = ", row['user_id']
            sliced_repos_for_user = self.repo_data[self.repo_data["owner_id"] == row["user_id"]]
            if sliced_repos_for_user.size == 0:
                print "Bad Slice of repo for user = " + str(row["user_id"])
                continue

            '''"user_id", "repo_count", "is_forked", "has_wiki", "cont_count", "forks_count", "subscribers_count", "language"'''

            self.user_repo_aggregated.set_value(index, "user_id", row["user_id"])
            self.user_repo_aggregated.set_value(index, "repo_count", row["repo_count"])

            self.user_repo_aggregated.set_value(index, "is_forked", self.aggregate_boolean_series(sliced_repos_for_user["is_forked"]))
            self.user_repo_aggregated.set_value(index, "has_wiki", self.aggregate_boolean_series(sliced_repos_for_user["has_wiki"]))
            self.user_repo_aggregated.set_value(index, "cont_count", self.aggregate_numerical_series(sliced_repos_for_user["cont_count"]))
            self.user_repo_aggregated.set_value(index, "forks_count", self.aggregate_numerical_series(sliced_repos_for_user["fork_count"]))
            self.user_repo_aggregated.set_value(index, "subscribers_count", self.aggregate_numerical_series(sliced_repos_for_user["sub_count"]))
            self.user_repo_aggregated.set_value(index, "language", self.aggregate_cat_series(sliced_repos_for_user["language"]))

        return

