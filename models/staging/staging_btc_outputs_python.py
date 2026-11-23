# Import the pandas library.
# pandas is used to manipulate and transform tabular data (DataFrames).
import pandas

# Import simplejson.
# simplejson is used to convert JSON-formatted text into Python objects.
import simplejson


# Every dbt Python model must have this function.
#
# dbt     = Provides dbt functionality, such as ref(), config(), and source().
# session = Provides access to the Snowflake Snowpark session.
#
# dbt calls this function when it executes the Python model.
def model(dbt, session):

    # Configure how dbt should execute this Python model.
    #
    # materialized="table":
    #     Save the final result as a physical table in Snowflake.
    #
    # packages:
    #     Tell Snowflake which Python packages the model needs.
    #     pandas    = Data manipulation.
    #     pyarrow   = Supports pandas data transfer through the
    #                 Snowflake Connector.
    #     simplejson = JSON parsing.
    #
    # These packages are provided in Snowflake's Python environment.
    dbt.config(
        materialized="table",
        packages=["pandas", "pyarrow", "simplejson"]
    )

    # Read data from the existing dbt model named stg_btc_transactions.
    #
    # dbt.ref():
    #     Identifies another dbt model as the upstream dependency.
    #
    # .to_pandas():
    #     Converts the Snowpark DataFrame returned by dbt.ref()
    #     into a pandas DataFrame.
    #
    # The resulting pandas DataFrame is stored in the variable df.
    #
    # This upstream model contains the OUTPUTS column with JSON data.
    df = dbt.ref("stg_btc_transactions").to_pandas()

    # DEBUGGING:
    # This line intentionally raises an exception to display
    # the data type of df in the dbt execution logs.
    #
    # We keep it commented out because we do not want the model
    # to stop with an intentional error during normal execution.
    #
    # When uncommented, it should display:
    # <class 'pandas.core.frame.DataFrame'>
    # raise Exception(f"DEBUG: df type is {type(df)}")


    # Find the OUTPUTS column in df.
    #
    # apply():
    #     Applies a function to every value in the OUTPUTS column.
    #
    # simplejson.loads:
    #     Converts each JSON string into a native Python object,
    #     such as a list containing dictionaries.
    #
    # Before:
    #     OUTPUTS = '[{"address": "bc1...", "value": 0.5}]'
    #
    # After:
    #     OUTPUTS = [{"address": "bc1...", "value": 0.5}]
    #
    # This makes the JSON data easier to manipulate with pandas.
    df["OUTPUTS"] = df["OUTPUTS"].apply(simplejson.loads)


    # Expand the OUTPUTS lists into separate DataFrame rows.
    #
    # explode():
    #     Creates a separate row for each item in the OUTPUTS list.
    #
    # For example, if one transaction has three outputs,
    # that transaction becomes three rows.
    #
    # reset_index(drop=True):
    #     Creates a new sequential index (0, 1, 2, 3...).
    #     drop=True prevents the old index from becoming a column.
    #
    # The expanded DataFrame is stored in df_exploded.
    df_exploded = df.explode("OUTPUTS").reset_index(drop=True)


    # Extract the address and value fields from each JSON object.
    #
    # pandas.json_normalize():
    #     Converts nested JSON dictionaries into tabular columns.
    #
    # Example JSON object:
    #     {"address": "bc1qexample", "value": 0.5}
    #
    # Becomes:
    #     address       value
    #     bc1qexample   0.5
    #
    # [["address", "value"]]:
    #     Selects only these two columns from the normalized data.
    #
    # The extracted columns are stored in df_outputs.
    df_outputs = pandas.json_normalize(
        df_exploded["OUTPUTS"]
    )[["address", "value"]]


    # Combine the original transaction columns with the extracted
    # address and value columns.
    #
    # pandas.concat():
    #     Combines multiple DataFrames.
    #
    # drop(columns="OUTPUTS"):
    #     Removes the original OUTPUTS JSON column because
    #     we have already extracted the information we need.
    #
    # axis=1:
    #     Combines the DataFrames horizontally (side by side).
    #
    # Result:
    #     Original transaction columns + address + value.
    #
    # The combined DataFrame is stored in df_final.
    df_final = pandas.concat(
        [
            df_exploded.drop(columns="OUTPUTS"),
            df_outputs
        ],
        axis=1
    )


    # Remove rows where the extracted address is NULL.
    #
    # notnull():
    #     Returns True when an address is present.
    #     Returns False when the address is missing.
    #
    # The filter keeps only rows with a non-null address.
    df_final = df_final[df_final["address"].notnull()]


    # Convert every column name to uppercase.
    #
    # Example:
    #     transaction_hash -> TRANSACTION_HASH
    #     block_timestamp  -> BLOCK_TIMESTAMP
    #     address          -> ADDRESS
    #     value            -> VALUE
    #
    # This follows the uppercase column naming convention
    # used in our Snowflake models.
    df_final.columns = [col.upper() for col in df_final.columns]


    # Return the final pandas DataFrame to dbt.
    #
    # dbt takes this returned DataFrame and writes its contents
    # into the Snowflake table configured above.
    #
    # Final table:
    # BTC_DEV.DBT_MARK.STAGING_BTC_OUTPUTS_PYTHON
    return df_final