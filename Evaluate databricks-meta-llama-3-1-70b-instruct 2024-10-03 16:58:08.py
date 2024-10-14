# Databricks notebook source
# MAGIC %md
# MAGIC ## Evaluate databricks-meta-llama-3-1-70b-instruct

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC This is the notebook version of your Playground session that used [Agent Evaluation](https://docs.databricks.com/generative-ai/agent-evaluation/index.html) to measure the quality of your Agent.  It includes:
# MAGIC 1. The requests you entered
# MAGIC 2. The code used to run Agent Evaluation on those requests.
# MAGIC
# MAGIC You can use this notebook to edit the requests in your evaluation dataset and to run evaluation on different versions of your agent, leveraging mlflow to track the computed quality metrics.

# COMMAND ----------

# MAGIC %pip install databricks-agents
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC # Agent endpoint
# MAGIC
# MAGIC Below, `endpoint` is the Model Serving endpoint you used in Playground. `agent` is a function that calls into the endpoint to get the response.
# MAGIC
# MAGIC *Note: If you want to run these requests against another Agents, `agent` can be a serving endpoint (`"endpoints:/..."`), a UC model (`"models:/..."`), an mlflow-registered model (`"runs:/..."`), or simply a function that calls into the agent. You can read more about these options in the [Agent Evaluation documentation](https://docs.databricks.com/generative-ai/agent-evaluation/evaluate-agent.html#example-agent-evaluation-runs-application).*

# COMMAND ----------

endpoint="databricks-meta-llama-3-1-70b-instruct"

import mlflow.deployments

def agent(input):
  client = mlflow.deployments.get_deploy_client("databricks")
  input.pop("databricks_options", None)
  return client.predict(endpoint=endpoint, inputs=input)

# COMMAND ----------

# MAGIC %md
# MAGIC # Requests
# MAGIC
# MAGIC Agent Evaluation can be used to either:
# MAGIC 1. Evaluate each single turn of conversation independently
# MAGIC 2. Evaluate the final turn of a multiple-turn conversation
# MAGIC
# MAGIC Using your requests, we show you both options below.  It is feasible to create evaluation datasets that mix both single- and multi-turn questions.  See [schema for request](https://docs.databricks.com/generative-ai/agent-evaluation/evaluation-set.html#schema-for-request).
# MAGIC
# MAGIC *Note: Agent Evaluation optionally allows you to provide a ground truth (or reference answer) to your questions.  If you do so, Agent Evaluation is able to assess the correctness (accuracy) of your Agent.  You can read more about the [schema of the evaluation dataset](https://docs.databricks.com/generative-ai/agent-evaluation/evaluation-set.html#evaluation-set-schema).*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Single turn
# MAGIC The following snippet turns your requests into an evaluation dataset with single-turn questions.  That is, each question will be sent to the Agent independently of one another.

# COMMAND ----------

# DBTITLE 1,Generate the evaluation dataset from the questions
import pandas as pd

examples =  [
    {
        "request": "You will be provided with a document and asked to summarize it.\n\nSummarize the following document:\n\nWith origins in academia and the open source community, Databricks was founded in 2013 by the original creators of Apache Spark™, Delta Lake and MLflow. As the world's first and only lakehouse platform in the cloud, Databricks combines the best of data warehouses and data lakes to offer an open and unified platform for data and AI.\n\nToday, more than 9,000 organizations worldwide — including ABN AMRO, Condé Nast, Regeneron and Shell — rely on Databricks to enable massive-scale data engineering, collaborative data science, full-lifecycle machine learning and business analytics.\n\nHeadquartered in San Francisco, with offices around the world and hundreds of global partners, including Microsoft, Amazon, Tableau, Informatica, Capgemini and Booz Allen Hamilton, Databricks is on a mission to simplify and democratize data and AI, helping data teams solve the world's toughest problems.",
        "expected_response": None  # Optional, fill in if you have a reference answer
    },
    # Add more questions here as needed, e.g.
    # {
    #     "request": "What is the capital of France?",
    #     "expected_response": "Paris"
    # },
]
eval_dataset = pd.DataFrame(examples)
display(eval_dataset)

# COMMAND ----------

# DBTITLE 1,Run evaluation and review the results
import mlflow
result = mlflow.evaluate(
    agent,
    data=eval_dataset,              # Your evaluation dataset
    model_type="databricks-agent",  # Enable Mosaic AI Agent Evaluation
)

# Review the evaluation results in the MLFLow UI (see console output), or access them in place:
display(result.tables['eval_results'])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Multi-turn questions
# MAGIC
# MAGIC The following snippet turns your requests into an evaluation dataset with multi-turn questions.  That is, Agent Evaluation will send the entire conversation to your Agent and *only* evaluate the Agent's response to the last request.

# COMMAND ----------

# DBTITLE 1,Generate the evaluation dataset from the full chat history

examples_from_chat = [
    {
        "request": {
            "messages": [
                {
                    "role": "user",
                    "content": "You will be provided with a document and asked to summarize it.\n\nSummarize the following document:\n\nWith origins in academia and the open source community, Databricks was founded in 2013 by the original creators of Apache Spark™, Delta Lake and MLflow. As the world's first and only lakehouse platform in the cloud, Databricks combines the best of data warehouses and data lakes to offer an open and unified platform for data and AI.\n\nToday, more than 9,000 organizations worldwide — including ABN AMRO, Condé Nast, Regeneron and Shell — rely on Databricks to enable massive-scale data engineering, collaborative data science, full-lifecycle machine learning and business analytics.\n\nHeadquartered in San Francisco, with offices around the world and hundreds of global partners, including Microsoft, Amazon, Tableau, Informatica, Capgemini and Booz Allen Hamilton, Databricks is on a mission to simplify and democratize data and AI, helping data teams solve the world's toughest problems."
                }
            ]
        },
        "expected_response": None
    }
]
eval_dataset_from_chat = pd.DataFrame(examples_from_chat)
display(eval_dataset_from_chat)

# COMMAND ----------

# DBTITLE 1,Run evaluation and review the results from the full chat history
result_from_chat = mlflow.evaluate(
    agent,
    data=eval_dataset_from_chat,    # Your evaluation dataset
    model_type="databricks-agent",  # Enable Mosaic AI Agent Evaluation
)

# Review the evaluation results in the MLFLow UI (see console output), or access them in place:
display(result_from_chat.tables['eval_results'])
