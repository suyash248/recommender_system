import math, time
from service.neo4jconnector import neo4j_con
from service.spark_context import get_or_create_spark_context

def load_neo4j_rdd(sc, neo4j_count_query, neo4j_main_query, offset=0, limit=1500):
    neo4j_rdd = sc.parallelize([], 4)

    total_count = neo4j_con.run(neo4j_count_query).evaluate() or 0
    pages = total_count/limit + (1 if total_count%limit else 0)
    print "Stats - Total record(s): {total_count} | Limit: {limit} | Total pages/pass:{pages}".format(
        total_count=total_count, limit=limit, pages=pages)
    start_time = time.asctime()
    print "Starting @ ", start_time
    for i in xrange(0, pages):
        params = {"offset": offset, "limit": limit}
        cur = neo4j_con.run(neo4j_main_query, params)
        nodes = [{ k: v for k, v in node.values()[0].iteritems()} for node in cur]

        #time.sleep(2)
        offset += limit
        # from py2neo.types import Node
        # Node().iteritems()
        print "Loaded {} nodes to RDD @ {}".format(offset+limit, time.asctime())
        neo4j_rdd = neo4j_rdd.union(sc.parallelize(nodes))
        break
    print "Finished at ", time.asctime()
    print "Processed {} records and loaded in RDD. Started @ {} Finished @ {}"\
        .format(total_count, start_time, time.asctime())
    return neo4j_rdd

def lamb(p):
    print p

if __name__ == '__main__':
    neo4j_count_query = "MATCH (prod:Product) return COUNT(prod)"
    neo4j_main_query = "MATCH (prod:Product) return prod SKIP {offset} LIMIT {limit}"
    sc = get_or_create_spark_context("RecommenderSystem")
    neo4j_rdd = load_neo4j_rdd(sc, neo4j_count_query, neo4j_main_query)
    #print neo4j_rdd.map(lamb).count()