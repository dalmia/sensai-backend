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
                    print("no - Quiz 3")
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


def save_daily_traces():
    from phoenix.client import Client

    if settings.env != "production":
        # only run in production
        return

    # Process previous day from 00:00:00 to 23:59:59
    previous_day = datetime.now() - timedelta(days=1)
    start_date = previous_day.replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = previous_day.replace(hour=23, minute=59, second=59, microsecond=0)

    print(
        f"Processing data for {start_date.strftime('%Y-%m-%d %H:%M')} to {end_date.strftime('%Y-%m-%d %H:%M')}",
        flush=True,
    )

    os.environ["PHOENIX_COLLECTOR_ENDPOINT"] = settings.phoenix_endpoint
    os.environ["PHOENIX_API_KEY"] = settings.phoenix_api_key

    phoenix_client = Client()

    df = phoenix_client.spans.get_spans_dataframe(
        project_name=f"sensai-{settings.env}",
        start_time=start_date,
        end_time=end_date,
        timeout=1200,
        limit=100000,
    )

    print(f"Got {len(df)} spans", flush=True)

    # Save dataframe to temporary local file
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", delete=False
    ) as temp_file:
        df.to_csv(temp_file.name, index=False)
        temp_filepath = temp_file.name

    # Upload to S3
    temp_filename = f"{start_date.strftime('%Y-%m-%d')}"
    s3_key = f"{settings.s3_folder_name}/phoenix/spans/{temp_filename}.csv"

    print(f"Saved to {temp_filepath}", flush=True)

    upload_file_to_s3(temp_filepath, s3_key)

    # Clean up temporary file
    os.remove(temp_filepath)

    print(f"Uploaded {len(df)} spans to S3 at key: {s3_key}", flush=True)

    feedback_traces_for_annotation_df = prepare_feedback_traces_for_annotation(df)

    feedback_conversations = feedback_traces_for_annotation_df.apply(
        convert_feedback_span_to_conversations, axis=1
    ).values.tolist()

    s3_key = f"{settings.s3_folder_name}/evals/conversations.json"

    with tempfile.NamedTemporaryFile(
        mode="wb", suffix=".json", delete=False
    ) as temp_file:
        file_bytes = download_file_from_s3_as_bytes(s3_key)
        temp_file.write(file_bytes)
        temp_filepath = temp_file.name

    conversations = json.loads(open(temp_filepath, "r").read())
    os.remove(temp_filepath)

    # Create backup with proper file handling
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False
    ) as temp_file:
        json.dump(conversations, temp_file, indent=4)
        temp_file.flush()  # Ensure all data is written to disk
        backup_filepath = temp_file.name

    upload_file_to_s3(
        backup_filepath,
        s3_key.replace(".json", "_backup.json"),
        content_type="application/json",
    )
    os.remove(backup_filepath)

    all_span_ids = set([c["id"] for c in conversations])
    new_count = 0

    for conversation in feedback_conversations:
        if conversation["id"] not in all_span_ids:
            new_count += 1
            conversations.append(conversation)

    # Save updated conversations with proper file handling
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False
    ) as temp_file:
        json.dump(conversations, temp_file, indent=4)
        temp_file.flush()  # Ensure all data is written to disk
        final_filepath = temp_file.name

    upload_file_to_s3(final_filepath, s3_key, content_type="application/json")
    os.remove(final_filepath)

    print(
        f"Uploaded {new_count} new feedback conversations to S3 at key: {s3_key}",
        flush=True,
    )
