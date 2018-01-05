import csv
from service.neo4jconnector import neo4j_con


def parse_and_import(file_path, chunk_size = 500):
    products_reader = list(csv.reader(open(file_path, 'rb'), delimiter='\t'))
    params = []
    count = 0

    stmt = """UNWIND {datas} AS data 
MERGE (prod1:`Product`{id: data.prod_id1})
MERGE (prod2:`Product`{id: data.prod_id2})
MERGE (prod1) -[:BOUGHT_WITH]-> (prod2)
    """
    print "Preparing data from file...", file_path
    for prod_ids in products_reader:
        prod_id1, prod_id2 = prod_ids[0], prod_ids[1]
        params.append({"prod_id1": prod_id1, "prod_id2": prod_id2})
        count += 1
        if count % chunk_size == 0:
            neo4j_con.run(stmt, {"datas": params})
            print "Imported {} record(s) successfully!".format(len(params))
            count = 0
            params = []

    neo4j_con.run(stmt, {"datas": params})
    print "Imported {} record(s) successfully!".format(len(params))


if __name__ == '__main__':
    file_path = "/Users/suyash.soni/Code/Recommender/public/amazon0302.txt"
    parse_and_import(file_path)

