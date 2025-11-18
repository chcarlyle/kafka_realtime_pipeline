# IDS706 Kafka Assignment

This demo streams synthetic hospital appointment data from Kafka into Postgres and surfaces it in a Streamlit dashboard tailored to clinical + financial ops.

## Data model (per appointment)
- `appointment_id`: short UUID
- `status`: Scheduled, Completed, Cancelled, No-Show
- `department`: e.g., Cardiology, Neurology
- `urgency`: Low, Medium, High, Critical
- `cost`: total visit cost (base + copay)
- `copay`: patient copay portion
- `timestamp`: event time for resampling trends
- `city`: facility location
- `payment_method`: Insurance, Self-Pay, Medicare, Medicaid

## Dashboard visuals
- KPIs: total appointments, total/avg cost, completion rate, avg copay.
- Patient Flow & Revenue: appointments per minute (bars) overlaid with revenue (line).
- Care Pathway Status Mix: horizontal bar counts by status.
- Urgency Mix by Department: stacked bars to spot high-acuity load.
- Payment Mix: avg cost vs avg copay by payment method.
- Raw data preview: top 10 latest records for quick inspection.

## The short video showing successful live dashboard updates is included in `LiveUpdates.mov`
