import argparse

from src.model import KMeansModel
from src.spark_connector import SparkConnector


def main(args):
    sc = SparkConnector()
    model = KMeansModel(args.k)
    result = model.fit(sc.read(args.url))
    sc.write(result, args.url)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-k', type=int, default=5)
    parser.add_argument('--url', type=str, required=True, default='jdbc:postgresql://localhost:5432/mle_hw3')
    main(parser.parse_args())
