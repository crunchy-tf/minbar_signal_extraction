# Minbar Signal Extraction Service

This microservice aggregates NLP insights (such as sentiment and keywords) from processed documents into unified topic-based signals over defined timeframes. These aggregated signals are then stored in a TimescaleDB database for time-series analysis and visualization.

The primary workflow for signal extraction is handled by a scheduled background job. An API endpoint is also provided, mainly for testing the aggregation logic with specific data.

## API Endpoints

---

### Signal Extraction Endpoint

#### `POST /extract-signal`
-   **Description**: Manually triggers the signal extraction and aggregation logic for a given topic and set of documents within a specified timeframe. This endpoint is primarily intended for testing the aggregation logic. The aggregated signal is returned and also stored in the target TimescaleDB.
-   **Request Body**: `SignalExtractionRequest`
    -   `topic_id` (string or integer, required): Identifier for the topic.
    -   `topic_name` (string, required): Human-readable name of the topic.
    -   `documents` (list of `ProcessedDocumentInput`, required): A list of documents to be aggregated for this signal. Each document should contain:
        -   `raw_mongo_id` (string, required): Original MongoDB ID of the document.
        -   `original_timestamp` (datetime string, required): Timestamp of the original document.
        -   `overall_sentiment` (list of `SentimentScoreEntry`, required): Sentiment scores for the document (e.g., `[{"label": "Concerned", "score": 0.7}]`).
        -   `extracted_keywords_frequency` (list of objects, required): Extracted keywords and their frequencies (e.g., `[{"keyword": "fever", "frequency": 2}]`).
    -   `timeframe_start` (datetime string, required): The start of the aggregation timeframe.
    -   `timeframe_end` (datetime string, required): The end of the aggregation timeframe.
-   **Sample Request Body**:
    ```json
    {
      "topic_id": "topic_123_healthcare_costs",
      "topic_name": "123_healthcare_costs_tunisia",
      "documents": [
        {
          "raw_mongo_id": "680a75cf622ece5a1dc7a4bb",
          "original_timestamp": "2024-04-17T10:00:00Z",
          "overall_sentiment": [
            {"label": "Concerned", "score": 0.85},
            {"label": "Anxious", "score": 0.6}
          ],
          "extracted_keywords_frequency": [
            {"keyword": "prix", "frequency": 3},
            {"keyword": "médicaments", "frequency": 2}
          ]
        },
        {
          "raw_mongo_id": "680a75cf622ece5a1dc7a4cc",
          "original_timestamp": "2024-04-17T10:30:00Z",
          "overall_sentiment": [
            {"label": "Angry", "score": 0.7},
            {"label": "Concerned", "score": 0.5}
          ],
          "extracted_keywords_frequency": [
            {"keyword": "assurance", "frequency": 2},
            {"keyword": "coût", "frequency": 1}
          ]
        }
      ],
      "timeframe_start": "2024-04-17T10:00:00Z",
      "timeframe_end": "2024-04-17T11:00:00Z"
    }
    ```
-   **Response Body**: `ExtractedSignal`
    -   Contains details of the aggregated signal, including:
        -   `signal_timestamp` (datetime)
        -   `topic_id` (string or integer)
        -   `topic_name` (string)
        -   `document_count` (integer)
        -   `aggregated_sentiment` (object with `average_scores`, `dominant_sentiment_label`, `dominant_sentiment_score`)
        -   `top_aggregated_keywords` (list of objects with `keyword`, `total_frequency`, `document_frequency`)
        -   `timeframe_start` (datetime)
        -   `timeframe_end` (datetime)

---