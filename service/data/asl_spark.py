# export JAVA_HOME='/Library/Java/JavaVirtualMachines/jdk1.8.0_152.jdk/Contents/Home'

import math, time, json
from copy import deepcopy
from service.neo4jconnector import neo4j_con
from service.spark_context import get_or_create_spark_context

def load_neo4j_rdd(sc, neo4j_main_query, offset=0, limit=1500):
    neo4j_rdd = sc.parallelize([], 4)

    # total_count = neo4j_con.run(neo4j_count_query).evaluate() or 0
    # pages = total_count/limit + (1 if total_count%limit else 0)
    # print "Stats - Total record(s): {total_count} | Limit: {limit} | Total pages/pass:{pages}".format(
    #     total_count=total_count, limit=limit, pages=pages)
    start_time = time.asctime()
    print "Starting @ ", start_time
    #for i in xrange(0, pages):
    while True:
        params = {"offset": offset, "limit": limit}
        cur = neo4j_con.run(neo4j_main_query, params)
        #nodes = [{ k: v for k, v in node.values()[0].iteritems()} for node in cur]
        nodes = []
        empty = True
        for node_tups in cur:
            empty = False
            nodes_list = []
            for node_tup in node_tups.values():
                if type(node_tup) == dict:
                    nodes_dict = {k: v for k, v in node_tup.iteritems()}
                    nodes_list.append(nodes_dict)
                else:
                    nodes_list.append(node_tup)
            nodes.append(deepcopy(nodes_list))

        offset += limit
        if empty:
            break
        # from py2neo.types import Node
        # Node().iteritems()
        print "Loaded {} nodes to RDD @ {}".format(offset+limit, time.asctime())
        neo4j_rdd = neo4j_rdd.union(sc.parallelize(nodes))

    print "Finished at ", time.asctime()
    print "Loaded records in RDD. Started @ {} Finished @ {}"\
        .format(start_time, time.asctime())
    return neo4j_rdd

def lamb(p):
    print p


def test():
    cur = neo4j_con.run("MATCH (p:Product)-[r:REVIEWED_BY]->(c:Customer) RETURN c")
    nodes = []
    count = 0
    for node_tups in cur:
        count += 1
        nodes_list = []
        for node_tup in node_tups.values():
            print node_tup
            if type(node_tup) == dict:
                nodes_dict = {k: v for k, v in node_tup.iteritems()}
                nodes_list.append(nodes_dict)
            else:
                nodes_list.append(node_tup)
        nodes.append(deepcopy(nodes_list))

    print json.dumps(nodes)
    print count

if __name__ == '__main__':
    # test()
    # neo4j_main_query = "MATCH (prod:Product) return prod SKIP {offset} LIMIT {limit}"

    neo4j_q = "MATCH (p:Product)-[r:REVIEWED_BY]->(c:Customer) RETURN c.num_id, p.product_id, r.rating SKIP {offset} LIMIT {limit}"
    sc = get_or_create_spark_context("RecommenderSystem")
    small_ratings_data = load_neo4j_rdd(sc, neo4j_q)
    print json.dumps(small_ratings_data.take(1))

    neo4j_q = "MATCH (prod:Product) WHERE prod.product_id > 0 return prod.product_id, prod.title SKIP {offset} LIMIT {limit}"
    sc = get_or_create_spark_context("RecommenderSystem")
    small_prods_data = load_neo4j_rdd(sc, neo4j_q)
    print json.dumps(small_prods_data.take(1))

    training_RDD, validation_RDD, test_RDD = small_ratings_data.randomSplit([6, 2, 2], seed=0L)
    validation_for_predict_RDD = validation_RDD.map(lambda x: (x[0], x[1]))
    test_for_predict_RDD = test_RDD.map(lambda x: (x[0], x[1]))



    from pyspark.mllib.recommendation import ALS

    # splits = small_ratings_data.randomSplit([0.8, 0.2], seed=0L)
    # trainingRatingsRDD = splits[0].cache()
    # testRatingsRDD = splits[1].cache()
    #
    # numTraining = trainingRatingsRDD.count()
    # numTest = testRatingsRDD.count()
    # model = ALS.train(training_RDD, 20, iterations=10)




    seed = 5L
    iterations = 10
    regularization_parameter = 0.1
    ranks = [4, 8, 12]
    errors = [0, 0, 0]
    err = 0
    tolerance = 0.02

    min_error = float('inf')
    best_rank = -1
    best_iteration = -1
    print "C0**********************************************"
    for rank in ranks:
        model = ALS.train(training_RDD, rank, iterations=iterations,
                          lambda_=regularization_parameter)
        print "C00**********************************************"
        predictions = model.predictAll(validation_for_predict_RDD).map(lambda r: ((r[0], r[1]), r[2]))
        print "C1**********************************************"
        rates_and_preds = validation_RDD.map(lambda r: ((int(r[0]), int(r[1])), float(r[2]))).join(predictions)
        print "C2**********************************************"
        error = math.sqrt(rates_and_preds.map(lambda r: (r[1][0] - r[1][1]) ** 2).mean())
        print "C3**********************************************"
        errors[err] = error
        err += 1
        print 'For rank %s the RMSE is %s' % (rank, error)
        if error < min_error:
            min_error = error
            best_rank = rank

    print 'The best model was trained with rank %s' % best_rank

    model = ALS.train(training_RDD, best_rank, seed=seed, iterations=iterations,
                      lambda_=regularization_parameter)
    predictions = model.predictAll(test_for_predict_RDD).map(lambda r: ((r[0], r[1]), r[2]))
    rates_and_preds = test_RDD.map(lambda r: ((int(r[0]), int(r[1])), float(r[2]))).join(predictions)
    error = math.sqrt(rates_and_preds.map(lambda r: (r[1][0] - r[1][1]) ** 2).mean())

    print 'For testing data the RMSE is %s' % (error)





