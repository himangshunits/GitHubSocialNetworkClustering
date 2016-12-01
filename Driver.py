from TrainFlowManager import TrainFlowManager
import graphlab as gl

def main():
    my_tr = TrainFlowManager()

    sf = gl.SFrame(my_tr.user_repo_aggregated)

    model = gl.kmeans.create(sf, num_clusters=6)
    #model.show()

    model['cluster_info'].print_rows(num_columns=5, max_row_width=80,
                                            max_column_width=10)


    print model['cluster_info'][['cluster_id', 'size', 'sum_squared_distance']]


    print model['cluster_id'].head()








if __name__ == '__main__':
    main()