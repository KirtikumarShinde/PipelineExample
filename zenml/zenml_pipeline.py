import pandas as pd
from zenml.steps import step, Output
from sklearn.impute import SimpleImputer
from sklearn.base import ClassifierMixin

# local imports
import sys
sys.path.append("/Users/adrien_couetoux/Documents/Sandbox/test_zenml")

from conf.parameters import ModelConfig, ImporterConfig

from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score

@step
def dynamic_importer(
    config: ImporterConfig,
) -> Output(
    df=pd.DataFrame
):
    """Downloads the latest data from a mock API."""
    return pd.read_csv(config.data_path)


# Add another step
@step
def impute_data(
    data: pd.DataFrame
) -> Output(data_normalized=pd.DataFrame):
    """Imputes data"""
    imputed_data = SimpleImputer().fit_transform(data)
    return pd.DataFrame(data=imputed_data, columns=data.columns)


@step
def model_trainer(
    config: ModelConfig,  # not an artifact; used for quickly changing params in runs
    data: pd.DataFrame,
) -> ClassifierMixin:
    """Train a neural net from scratch to recognize MNIST digits return our
    model or the learner"""
    model = RandomForestClassifier(n_estimators=config.n_estimators)

    features = [x for x in data.columns if x!=config.target_col]
    X = data[features]
    y = data[config.target_col]
    model.fit(X,y)

    # model just needs to be returned, zenml takes care of persisting the model
    return model

@step
def model_accuracy(
    data: pd.DataFrame,
    model: ClassifierMixin,
) -> float:
    """Calculate the loss for the model on the test set"""

    features = [x for x in data.columns if x!="has_diabetes"]
    preds = model.predict(data[features])
    accuracy = accuracy_score(preds, data["has_diabetes"])
    return accuracy

from zenml.pipelines import pipeline

@pipeline
def diabetes_pipeline(
    importer,
    preprocessor,
    trainer,
    evaluator
):
    """The simplest possible pipeline"""
    # We just need to call the function
    data = importer()
    preprocessed_data = preprocessor(data)
    fitted_model=trainer(preprocessed_data)
    accuracy=evaluator(preprocessed_data, fitted_model)

# run the pipeline
diabetes_pipeline(
    importer=dynamic_importer(),
    preprocessor=impute_data(),
    trainer=model_trainer(config=ModelConfig(n_estimators=10)),
    evaluator=model_accuracy(),
    ).run()