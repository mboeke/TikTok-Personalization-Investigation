from Analysis_Methods import *
from Analysis_Text_Methods import *
from SkipGramModel import *
from SkipGramModelEvaluation import *

# TEST SCENARIOS:
action_type = 'Like'
all_test_user_ids_like = [[22, 24], [25, 26], [27, 28], [29, 30], [31, 32], [33, 34], [35, 36], [45, 46], [59, 60],
                          [61, 62], [63, 64], [70, 71], [111, 112], [113, 114], [115, 116], [117, 118], [123, 124],
                          [135, 136], [159, 160]]
tests_like_5_batches = [[45, 46], [59, 60], [61, 62], [63, 64], [70, 71]]
tests_like_3_batches = [[113, 114], [135, 136], [115, 116], [117, 118], [123, 124], [159, 160]]
excluded_like_users = [[22, 24], [25, 26], [27, 28], [29, 30], [31, 32], [33, 34], [35, 36], [111, 112]]

# action_type = 'Follow'
all_test_follow_3_batches = [[47, 48], [49, 50], [53, 54], [153, 154], [155, 156]]
excluded_follow_users = [[51, 52]]

# action_type = 'Video View Rate'
all_test_vvr_3_batches = [[77, 78], [79, 80], [81, 82], [83, 84], [85, 86], [87, 88], [91, 92], [145, 146], [151, 152],
                          [157, 158]]
# tests_vvr_personas_3_batches = [[87, 88], [91, 92], [145, 146], [151, 152], [157, 158]]
excluded_vvr_users = [[89, 90], [127, 128]]

# action_type = 'Control Group'
excluded_control_groups_5_batches = [[93, 94]]
new_control_group_3_batches = [[143, 144], [147, 148], [149, 150]]
control_group_5_batches = [[72, 73], [74, 75], [95, 96]]  # [38, 39], [40, 41], [55, 56], [57, 58], [65, 67], [68, 69],
control_group_3_batches = [[125, 126], [137, 138], [139, 140], [141, 142], [143, 144], [147, 148], [149, 150]]  # [119, 120], [121, 122]

# action_type = 'Location'
diff_country_same_language_3_batches = [97, 98, 99, 100]
# diff_country_same_language_switching_country_loc_3_batches = [101, 102, 105, 106]  # => EXCLUDED !
diff_country_diff_language_switching_country_3_batches = [103, 104, 107, 108]
same_country_diff_language_3_batches = [109, 110, 129, 130, 131, 132, 133, 134]

# action_type = "Collaborative Filtering"
collaborative_filtering_groups = [[87, 88], [87, 91], [87, 92], [87, 123], [87, 124], [87, 145], [87, 146], [87, 151],
                                  [87, 152], [87, 157], [87, 158], [87, 159], [87, 160], [88, 91], [88, 92], [88, 123],
                                  [88, 124], [88, 145], [88, 146], [88, 151], [88, 152], [88, 157], [88, 158],
                                  [88, 159], [88, 160], [91, 92], [91, 123], [91, 124], [91, 145], [91, 146], [91, 151],
                                  [91, 152], [91, 157], [91, 158], [91, 159], [91, 160], [92, 123], [92, 124],
                                  [92, 145], [92, 146], [92, 151], [92, 152], [92, 157], [92, 158], [92, 159],
                                  [92, 160], [123, 124], [123, 145], [123, 146], [123, 151], [123, 152], [123, 157],
                                  [123, 158], [123, 159], [123, 160], [124, 145], [124, 146], [124, 151], [124, 152],
                                  [124, 157], [124, 158], [124, 159], [124, 160], [145, 146], [145, 151], [145, 152],
                                  [145, 157], [145, 158], [145, 159], [145, 160], [146, 151], [146, 152], [146, 157],
                                  [146, 158], [146, 159], [146, 160], [151, 152], [151, 157], [151, 158], [151, 159],
                                  [151, 160], [152, 157], [152, 158], [152, 159], [152, 160], [157, 158], [157, 159],
                                  [157, 160], [158, 159], [158, 160], [159, 160]]

test_groups = {
    'Like': {
        'users': [tests_like_5_batches, tests_like_3_batches],
        'batch': [5, 3]
    },
    'Follow': {
        'users': [all_test_follow_3_batches],
        'batch': [3]
    },
    'Video View Rate': {
        'users': [all_test_vvr_3_batches],
        'batch': [3]
    },
    'Control Group': {
        'users': [control_group_5_batches, control_group_3_batches],
        'batch': [5, 3]
    },
    'Location': {
        'users': [diff_country_same_language_3_batches],
        'batch': [3]
    }
}

test_groups = {
    'Location': {
        'users': [diff_country_same_language_3_batches],
        'batch': [3]
    }
}

if __name__ == '__main__':

    noise_all_computation_5, noise_run_computation_5, noise_avg_overall_runs_overall_users_computation_5 = \
          compute_noise_control_scenarios(control_group_5_batches, 5)
    noise_all_computation_3, noise_run_computation_3, noise_avg_overall_runs_overall_users_computation_3 = \
        compute_noise_control_scenarios(control_group_3_batches[:len(control_group_3_batches)-2], 3, False)
    # noise_all_computation_3_unfinished, noise_run_computation_3_unfinished = \
    #     compute_noise_control_scenarios(control_group_3_batches, 3, True)

    # # Initializing SkipGramEvaluation Class to utilize for analysis
    # test_data_values, test_data_dict = get_test_data(test_users=[53, 54, 91, 92, 123, 124])
    # training_data = get_training_data(test_hashtags=test_data_dict)
    # embedding_size = 300
    # skip_gram_model_evaluation = SkipGramModelEvaluation(embedding_size=embedding_size, test_data=test_data_dict,
    #                                                      frequencies=[100, 500, 1000], epochs=5, lr=0.1,
    #                                                      max_freq=100000, min_freq=2)

    for group in test_groups:
        for batch in test_groups.get(group).get('batch'):
            cur_index = test_groups.get(group).get('batch').index(batch)
            test_group = {
                "Action_Type": group,
                "Batch_Size": batch,
                "Users": test_groups.get(group).get('users')[cur_index],
                "Account_Of_Unfinished_Scenarios": False
            }

            # DIFFERENCE ANALYSIS OF POSTS OF LOCATION TESTS
            if test_group.get('Action_Type') == "Location":
                heatmap_location(diff_country_same_language_3_batches, noise_avg_overall_runs_overall_users_computation_3)
                heatmap_location(diff_country_diff_language_switching_country_3_batches,
                                 noise_avg_overall_runs_overall_users_computation_3, switching_loc=True)
                heatmap_location(same_country_diff_language_3_batches, noise_avg_overall_runs_overall_users_computation_3)

            # TRAINING SKIP-GRAM MODEL FOR SIMILARITY ANALYSIS
            # test_data_values, test_data_dict = get_test_data(test_users=[53, 54, 91, 92, 123, 124])
            # training_data = get_training_data(test_hashtags=test_data_dict)
            # embedding_size = 300
            # print("***** Training and test data fetched *****")
            # for epochs in [9, 10]:
            #     for lr in [0.1]:
            #         print(f"***** Starting Iteration with {epochs} epochs and lr = {lr}")
            #         skip_gram_model = SkipGramModel(max_freq=100000, min_freq=2, embedding_size=embedding_size,
            #                                         neg_sample_size=20, lr=lr, epochs=epochs, training_data=training_data)
            #         skip_gram_model_evaluation = SkipGramModelEvaluation(embedding_size=embedding_size, test_data=test_data_dict,
            #                                                              frequencies=[100, 500, 1000], epochs=epochs, lr=lr,
            #                                                              max_freq=100000, min_freq=2)
            #         visualize_similarities([123, 124], get_test_run_ids_2_user([123, 124])[:20], action_type,
            #                                skip_gram_model_evaluation, epochs, lr)

            else:
                # as some test scenarios did not complete all runs we have to reduce the number of test runs for which we calculate
                # the noises
                test_runs_to_consider = 0
                if test_group.get('Account_Of_Unfinished_Scenarios'):
                    number_of_test_runs = []
                    for pair in test_group.get('Users'):
                        # get test runs
                        test_runs = get_test_run_ids_2_user(pair)
                        number_of_test_runs.append(len(test_runs))
                    test_runs_to_consider = min(number_of_test_runs)

                for user_pair in test_group.get('Users'):
                    action_type = test_group.get('Action_Type')
                    noise = 0

                    if test_group.get('Batch_Size') == 3 and action_type != 'Control Group':
                        noise = noise_avg_overall_runs_overall_users_computation_3
                    elif test_group.get('Batch_Size') == 5 and action_type != 'Control Group':
                        noise = noise_avg_overall_runs_overall_users_computation_5
                    print(f"Utilized noise: {noise}")

                    # in general only consider the first 20 test runs of a test scenario
                    test_run_ids = get_test_run_ids_2_user(user_pair)[:20]
                    if test_group.get('Account_For_Unfinished_Scenarios'):
                        test_run_ids = test_run_ids[:test_runs_to_consider]
                    print(f"User pair: {user_pair}: {test_run_ids}")

                    action_user = get_action_user(user_pair)
                    print(f"*** ACTION USER IS {action_user}")

                    # # DIFFERENCE ANALYSIS OF POSTS, HASHTAGS, CONTENT CREATORS, SOUND analyze overlapping posts between two users
                    # difference_analysis(test_user_pair=user_pair, test_runs=test_run_ids,
                    #                     action_type=action_type, noise=noise, action_user=action_user,
                    #                     thesis_chart=False, account_for_drop=True)
                    #
                    # # POST METRICS ANALYSIS
                    # development_of_post_metrics(test_run_ids, user_pair, action_type, action_user, thesis_chart=False)

                    # REAPPEARANCE OF POST ATTRIBUTE ANALYSIS INCL DISTRIBUTION OF METRICS APPEARANCE OVER ALL TEST RUNS
                    for metric in ['Hashtag', 'Content Creator', 'Sound']:
                        reappearance_analysis_of_metric(test_user_pair=user_pair, test_runs=test_run_ids,
                                                        metric=metric, action_type=action_type,
                                                        action_user=action_user, thesis_chart=False)

                    # # SIMILARITY / DIFFERENCE OF HASHTAG ANALYSIS
                    visualize_similarities(user_pair, test_run_ids, action_type, skip_gram_model_evaluation, 5, 0.1,
                                            within_feed=True, thesis_chart=False)
                    # # generate_similarities_differences(test_user_pair=user_pair, test_runs=test_run_ids,
                    # #                                   action_type=action_type, hashtags=True,
                    # #                                   within_test_run=True, within_feed=False)

    # analyze gradient of post differences and error rate
    # generate_chart_error_rate_2_users(all_test_users=test_group.get('Users'), action_type=action_type)

