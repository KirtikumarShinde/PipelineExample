import pandas as pd
from metaflow import FlowSpec, step, Parameter

import sys

sys.path.append(
    "/Users/kirtikumar_shinde/Documents/PycharmProjects/PMPx/PipelineExamples"
)

from kedro_pipeline.src.kedro_pipeline.nodes import (
    split_data,
    make_predictions,
    report_accuracy,
)


class Pipeline_example(FlowSpec):

    path = Parameter(
        "config_file",
        help="data path",
        default="config_tree.yaml",
    )

    cgd = compose(confg_folder, config_file)

    @step
    def start(self):
        print(self.path)
        print(self.param)
        self.next(self.read_data)

    @step
    def read_data(self):
        self.pd_iris = pd.read_csv(self.path)
        print(self.pd_iris.head(2))
        self.next(self.call_split_data)

    @step
    def call_split_data(self):
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
        df = spark.createDataFrame(self.pd_iris)
        print(df.schema)

        output_list = split_data(self.pd_iris, self.param)
        self.X_train = output_list[0]
        self.X_test = output_list[1]
        self.y_train = output_list[2]
        self.y_test = output_list[3]
        print(self.y_test.head(2))
        self.next(self.call_make_predictions)

    @step
    def call_make_predictions(self):
        self.y_pred = make_predictions(self.X_train, self.X_test, self.y_train)
        print(self.y_pred.head(2))
        self.next(self.call_report_accuracy)

    @step
    def call_report_accuracy(self):
        self.report_accuracy = report_accuracy(self.y_pred, self.y_test)
        print(self.report_accuracy)
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    Pipeline_example()
