import pandas as pd
from metaflow import FlowSpec, step, Parameter

import sys

sys.path.append(
    # "/Users/kirtikumar_shinde/Documents/PycharmProjects/PMPx/PipelineExamples"
    "./"
)

from nodes import (
    split_data,
    make_predictions,
    report_accuracy,
)
import hydra
from hydra import compose, initialize
from omegaconf import DictConfig, OmegaConf


class Pipeline_example(FlowSpec):
    # Config file name - setup as metaflow variable
    #   default set to "config_tree.yaml"
    config_name = Parameter(
        "config_name",
        help="config_name tree",
        default="config_tree"
    )

    @step
    def start(self):
        # hydra.core.global_hydra.GlobalHydra.instance().clear()
        # hydra.initialize(config_path="config")
        # init hydra compose
        initialize(config_path="config", job_name="metahydra")
        self.params = compose(config_name=self.config_name, overrides=[])
        self.next(self.read_data)

    @step
    def read_data(self):
        print(self.params)
        self.pd_iris = pd.read_csv(self.params["parameters"]["path"])
        print(self.pd_iris.head(2))
        self.next(self.call_split_data)

    @step
    def call_split_data(self):
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
        df = spark.createDataFrame(self.pd_iris)
        print(df.schema)

        output_list = split_data(self.pd_iris, self.params["parameters"]["model_params"])
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
        print(self.__dict__)
        self.next(self.call_report_accuracy)

    @step
    def call_report_accuracy(self):
        report_accuracy(self.y_pred, self.y_test)
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    Pipeline_example()