import os
from datetime import datetime, timedelta
from typing import Optional
from datetime import datetime, timedelta
import tempfile
import json
import math
import pandas as pd
from api.settings import settings
from api.utils.s3 import upload_file_to_s3, download_file_from_s3_as_bytes
from api.utils.logging import logger


def get_raw_traces(
    filter_period: Optional[str] = None, timeout: int = 120
) -> pd.DataFrame:
    from phoenix import Client

    os.environ["PHOENIX_COLLECTOR_ENDPOINT"] = settings.phoenix_endpoint
    os.environ["PHOENIX_API_KEY"] = settings.phoenix_api_key
    project_name = f"sensai-{settings.env}"

    if not filter_period:
        return Client().get_spans_dataframe(project_name=project_name, timeout=timeout)

    if filter_period not in ["last_day", "current_month", "current_year"]:
        raise ValueError("Invalid filter period")

    if filter_period == "last_day":
        end_time = datetime.now()
        start_time = end_time - timedelta(days=1)
        return Client().get_spans_dataframe(
            project_name=project_name,
            start_time=start_time,
            end_time=end_time,
            timeout=timeout,
        )

    if filter_period == "current_month":
        end_time = datetime.now()
        start_time = end_time.replace(day=1)
        return Client().get_spans_dataframe(
            project_name=project_name,
            start_time=start_time,
            end_time=end_time,
            timeout=timeout,
        )

    end_time = datetime.now()
    start_time = end_time.replace(month=1, day=1)
    return Client().get_spans_dataframe(
        project_name=project_name,
        start_time=start_time,
        end_time=end_time,
        timeout=timeout,
    )


def prepare_feedback_traces_for_annotation(df: pd.DataFrame) -> pd.DataFrame:
    # Filter out feedback stage entries
    df_non_root = df[~df["attributes.metadata"].isna()].reset_index(drop=True)
    df_feedback = df_non_root[
        df_non_root["attributes.metadata"].apply(lambda x: x["stage"] == "feedback")
    ].reset_index(drop=True)

    # Function to get the last entry for each group and build chat history
    def get_last_entries_with_chat_history(df):
        # Separate learning_material and quiz types
        df_lm = df[
            df["attributes.metadata"].apply(
                lambda x: x.get("type") == "learning_material"
            )
        ]
        df_quiz = df[df["attributes.metadata"].apply(lambda x: x.get("type") == "quiz")]

        result_dfs = []

        # For learning_material: group by task_id and user_id
        if not df_lm.empty:
            df_lm_copy = df_lm.copy()
            df_lm_copy["task_id"] = df_lm_copy["attributes.metadata"].apply(
                lambda x: x.get("task_id")
            )

            # Group and process each group
            grouped_lm = df_lm_copy.groupby(["task_id", "attributes.user.id"])

            processed_rows = []
            for (task_id, user_id), group in grouped_lm:
                # Sort by start_time to ensure chronological order
                group_sorted = group.sort_values("start_time")

                # Build chat history from all entries in the group
                chat_history = []
                context = None
                for _, row in group_sorted.iterrows():
                    try:
                        input_messages = row["attributes.llm.input_messages"]
                        output_messages = row["attributes.llm.output_messages"]

                        # Find the second last user message (the actual user query)
                        user_messages = [
                            msg
                            for msg in input_messages
                            if msg.get("message.role") == "user"
                        ]
                        if (
                            "Reference Material"
                            not in user_messages[-1]["message.content"]
                        ):
                            continue

                        if context is None:
                            context = user_messages[-1]["message.content"]

                        if len(user_messages) >= 2:
                            user_message = user_messages[-2]["message.content"]

                            # Get AI response
                            if output_messages:
                                ai_message = json.loads(
                                    output_messages[0]["message.tool_calls"][0][
                                        "tool_call.function.arguments"
                                    ]
                                )

                                chat_history.append(
                                    {"role": "user", "content": user_message}
                                )
                                chat_history.append(
                                    {"role": "assistant", "content": ai_message}
                                )
                    except:
                        continue

                if not chat_history:
                    continue

                # Take the last entry and add chat history
                last_entry = group_sorted.iloc[-1].copy()
                last_entry["chat_history"] = chat_history
                last_entry["context"] = context
                processed_rows.append(last_entry)

            if processed_rows:
                df_lm_result = pd.DataFrame(processed_rows).drop(["task_id"], axis=1)
                result_dfs.append(df_lm_result)

        # For quiz: group by question_id and user_id
        if not df_quiz.empty:
            df_quiz_copy = df_quiz.copy()
            df_quiz_copy["question_id"] = df_quiz_copy["attributes.metadata"].apply(
                lambda x: x.get("question_id")
            )
            # Group and process each group
            grouped_quiz = df_quiz_copy.groupby(["question_id", "attributes.user.id"])

            processed_rows = []
            for (question_id, user_id), group in grouped_quiz:
                # Sort by start_time to ensure chronological order
                group_sorted = group.sort_values("start_time")

                # Build chat history from all entries in the group
                chat_history = []
                context = None
                for _, row in group_sorted.iterrows():
                    try:
                        input_messages = row["attributes.llm.input_messages"]

                        if isinstance(
                            row["attributes.llm.output_messages"], float
                        ) and math.isnan(row["attributes.llm.output_messages"]):
                            continue

                        output_messages = row["attributes.llm.output_messages"]

                        # Find the second last user message (the actual user query)
                        user_messages = [
                            msg
                            for msg in input_messages
                            if msg.get("message.role") == "user"
                        ]

                        if context is None:
                            context = user_messages[-1]["message.content"]

                        if len(user_messages) >= 2:
                            if "message.contents" in user_messages[-2]:
                                user_message = user_messages[-2]["message.contents"][0][
                                    "message_content.text"
                                ]
                            else:
                                user_message = user_messages[-2]["message.content"]

                            # Get AI response
                            if output_messages:
                                if "message.tool_calls" not in output_messages[0]:
                                    continue

                                try:
                                    ai_message = json.loads(
                                        output_messages[0]["message.tool_calls"][0][
                                            "tool_call.function.arguments"
                                        ]
                                    )
                                except:
                                    continue

                                chat_history.append(
                                    {"role": "user", "content": user_message}
                                )
                                chat_history.append(
                                    {"role": "assistant", "content": ai_message}
                                )
                    except Exception as e:
                        raise e

                if not chat_history:
                    continue

                # Take the last entry and add chat history
                last_entry = group_sorted.iloc[-1].copy()
                last_entry["chat_history"] = chat_history
                last_entry["context"] = context
                processed_rows.append(last_entry)

            if processed_rows:
                df_quiz_result = pd.DataFrame(processed_rows).drop(
                    ["question_id"], axis=1
                )
                result_dfs.append(df_quiz_result)

        # Combine all results
        if result_dfs:
            return pd.concat(result_dfs, ignore_index=True)
        else:
            return pd.DataFrame()

    return get_last_entries_with_chat_history(df_feedback)


def convert_feedback_span_to_conversations(row):
    conversation = {
        "id": row["context.span_id"],
        "start_time": row["start_time"].isoformat(),
        "end_time": row["end_time"].isoformat(),
        "uploaded_by": "Aman",
        "metadata": row["attributes.metadata"],
        "context": row["context"],
        "messages": row["chat_history"],
        "trace_id": row["context.trace_id"],
        "span_kind": row["span_kind"],
        "span_name": row["name"],
        "llm": {
            "model_name": row["attributes.llm.model_name"],
            "provider": row["attributes.llm.provider"],
        },
    }

    if isinstance(conversation["llm"]["provider"], float) and math.isnan(
        conversation["llm"]["provider"]
    ):
        conversation["llm"]["provider"] = None

    return conversation


def save_daily_traces(
    batch_size: int = 5,
):
    from phoenix import Client
    from phoenix.trace.dsl import SpanQuery

    if settings.env != "production":
        # only run in production
        return

    # Process previous day from 00:00:00 to 23:59:59
    previous_day = datetime.now(timezone(timedelta(hours=5, minutes=30))) - timedelta(
        days=1
    )
    start_date = previous_day.replace(hour=0, minute=0, second=0, microsecond=0)

    logger.info(
        f"Processing data for {start_date.strftime('%Y-%m-%d')}",
        flush=True,
    )

    os.environ["PHOENIX_COLLECTOR_ENDPOINT"] = settings.phoenix_endpoint
    os.environ["PHOENIX_API_KEY"] = settings.phoenix_api_key

    phoenix_client = Client()

    # Accumulate dataframes per hour
    dfs = []
    start_time = start_date
    end_time = start_date.replace(hour=23, minute=59, second=59, microsecond=0)

    current_time = start_time
    time_interval = timedelta(minutes=60)

    # For final feedback annotation, we need the full df at the end
    df = pd.DataFrame()

    # We'll use a folder per day, and store each query's CSV inside it
    day_folder = (
        f"{settings.s3_folder_name}/phoenix/spans/{start_date.strftime('%Y-%m-%d')}/"
    )

    count = 0

    while current_time < end_time:
        batch_start = current_time
        batch_end = min(
            batch_start + time_interval - timedelta(microseconds=1), end_time
        )

        batch_query_start = batch_start
        while True:
            logger.info(
                f"Fetching spans from {batch_query_start.strftime('%Y-%m-%d %H:%M:%S')} to {batch_end.strftime('%Y-%m-%d %H:%M:%S')}",
                flush=True,
            )

            q = (
                SpanQuery()
                # filter out long-running or mega-token spans ⇣
                .where(f"name == 'ChatCompletion'")
            )

            df_batch = phoenix_client.query_spans(
                q,
                project_name=f"sensai-{settings.env}",
                start_time=batch_query_start,
                end_time=batch_end,
                timeout=1200,
                limit=batch_size,
            )
            logger.info(f"Received {len(df_batch)} spans", flush=True)

            if df_batch.empty:
                break

            dfs.append(df_batch)
            count += len(df_batch)

            logger.info("Writing spans to file", flush=True)

            # Save this query's dataframe to a temporary local file
            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".csv", delete=False
            ) as temp_file:
                df_batch.to_csv(temp_file.name, index=False)
                temp_filepath = temp_file.name

            # Upload to S3: folder per day, file per query
            temp_filename = f"{batch_query_start.strftime('%Y-%m-%d_%H-%M-%S')}.csv"
            s3_key = f"{day_folder}{temp_filename}"

            upload_file_to_s3(temp_filepath, s3_key)

            # Clean up temporary file
            os.remove(temp_filepath)

            logger.info(
                f"Uploaded {len(df_batch)} spans to S3 at key: {s3_key}", flush=True
            )

            # If we got less than batch_size rows, there might be no more data in this batch window
            if len(df_batch) != batch_size:
                break

            # Use the end time of the last element as the new start time for the next query
            # To avoid missing any with the same timestamp, add 1 microsecond
            last_end_time = df_batch["end_time"].max()
            if pd.isnull(last_end_time):
                # Defensive: if end_time is missing, break to avoid infinite loop
                break

            # If end_time is a string, convert to datetime
            if isinstance(last_end_time, str):
                last_end_time = pd.to_datetime(last_end_time)

            # Ensure timezone consistency - make both timezone-naive for comparison
            if hasattr(last_end_time, "tz_localize"):
                # If it's a pandas Timestamp, convert to naive
                last_end_time = (
                    last_end_time.tz_localize(None)
                    if last_end_time.tz is not None
                    else last_end_time
                )
            elif hasattr(last_end_time, "tzinfo") and last_end_time.tzinfo is not None:
                # If it's a datetime with timezone, convert to naive
                last_end_time = last_end_time.replace(tzinfo=None)

            batch_query_start = last_end_time + timedelta(microseconds=1)

            # If the new start is after batch_end, we're done
            if batch_query_start > batch_end:
                break

        current_time = batch_end + timedelta(microseconds=1)

    logger.info(
        f"Total spans fetched: {count}, {sum(len(df) for df in dfs)}",
        flush=True,
    )

    df = pd.concat(dfs, ignore_index=True)

    feedback_traces_for_annotation_df = prepare_feedback_traces_for_annotation(df)

    conversations = feedback_traces_for_annotation_df.apply(
        convert_feedback_span_to_conversations, axis=1
    ).values.tolist()

    s3_key = f"{settings.s3_folder_name}/evals/conversations.json"

    # Save updated conversations with proper file handling
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False
    ) as temp_file:
        json.dump(conversations, temp_file)
        temp_file.flush()  # Ensure all data is written to disk
        final_filepath = temp_file.name

    upload_file_to_s3(final_filepath, s3_key, content_type="application/json")
    os.remove(final_filepath)

    logger.info(
        f"Uploaded {new_count} new feedback conversations to S3 at key: {s3_key}",
        flush=True,
    )
