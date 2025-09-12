import logging
import re
from typing import Any, Dict, Iterator, List, Tuple

from pydantic import ValidationError

from py_load_epar.models import (
    EparIndex,
    EparSubstanceLink,
    Organization,
    Substance,
)
from py_load_epar.spor_api.client import SporApiClient

logger = logging.getLogger(__name__)


def transform_and_validate(  # noqa: C901
    raw_records: Iterator[Dict[str, Any]],
    spor_client: SporApiClient,
    execution_id: int,
) -> Iterator[
    Tuple[EparIndex, List[EparSubstanceLink], List[Organization], List[Substance]]
]:
    """
    Transforms raw data, validates it, and enriches it with SPOR API data.

    - Validates raw data against the EparIndex Pydantic model.
    - Generates a stable `epar_id`.
    - Enriches organisation data using SPOR OMS.
    - Enriches substance data using SPOR SMS and creates link table records.
    - Captures the discovered master data (Organisations, Substances) to be loaded.
    - Records that fail validation are logged and skipped (quarantined).

    Args:
        raw_records: An iterator yielding dictionaries of raw EPAR data.
        spor_client: An instance of the SporApiClient for enrichment.
        execution_id: The current pipeline execution ID.

    Yields:
        A tuple containing:
        - The enriched EparIndex Pydantic model instance.
        - A list of EparSubstanceLink instances for that EPAR.
        - A list of discovered Organization models to be loaded.
        - A list of discovered Substance models to be loaded.
    """
    logger.info("Starting data transformation, validation, and enrichment.")
    validated_count = 0
    failed_count = 0

    for i, raw_record in enumerate(raw_records):
        try:
            # Use 'product_number' from source as the stable unique ID.
            product_number = raw_record.get("product_number")
            if not product_number:
                raise ValueError(
                    "Record is missing 'product_number', required for the stable ID."
                )
            raw_record["epar_id"] = str(product_number)
            raw_record["etl_execution_id"] = execution_id

            # 1. Validate the base record
            validated_model = EparIndex.model_validate(raw_record)
            substance_links: List[EparSubstanceLink] = []
            organizations: List[Organization] = []
            substances: List[Substance] = []

            # 2. Handle soft-deletes for withdrawn medicines
            withdrawn_statuses = ["withdrawn", "suspended"]
            if any(
                status in validated_model.authorization_status.lower()
                for status in withdrawn_statuses
            ):
                validated_model.is_active = False

            # 3. Enrich Organisation (MAH) and capture master data
            if validated_model.marketing_authorization_holder_raw:
                try:
                    org_api = spor_client.search_organisation(
                        validated_model.marketing_authorization_holder_raw
                    )
                    if org_api:
                        validated_model.mah_oms_id = org_api.org_id
                        # Map API model to DB model
                        org_db = Organization(
                            oms_id=org_api.org_id, organization_name=org_api.name
                        )
                        organizations.append(org_db)
                        logger.debug(
                            f"Enriched MAH '{org_api.name}' with OMS ID "
                            f"{org_api.org_id}"
                        )
                except Exception as e:
                    logger.error(
                        f"SPOR API error during organization search for "
                        f"'{validated_model.marketing_authorization_holder_raw}': {e}"
                    )

            # 4. Enrich Substances, create link records, and capture master data
            if hasattr(validated_model, "active_substance_raw") and validated_model.active_substance_raw:
                substance_names = re.split(
                    r"[,;]|\s+and\s+", validated_model.active_substance_raw
                )
                for sub_name_raw in substance_names:
                    sub_name = sub_name_raw.strip()
                    if not sub_name:
                        continue
                    try:
                        sub_api = spor_client.search_substance(sub_name)
                        if sub_api:
                            link = EparSubstanceLink(
                                epar_id=validated_model.epar_id,
                                spor_substance_id=sub_api.sms_id,
                            )
                            substance_links.append(link)
                            # Map API model to DB model
                            sub_db = Substance(
                                spor_substance_id=sub_api.sms_id,
                                substance_name=sub_api.name,
                            )
                            substances.append(sub_db)
                            logger.debug(
                                f"Enriched substance '{sub_api.name}' with SMS "
                                f"ID {sub_api.sms_id}"
                            )
                    except Exception as e:
                        logger.error(
                            f"SPOR API error during substance search for '{sub_name}': {e}"
                        )

            yield validated_model, substance_links, organizations, substances
            validated_count += 1

        except (ValidationError, KeyError) as e:
            logger.warning(
                f"Record {i+1} failed validation or has missing key. Record: {raw_record}. Error: {e}"
            )
            failed_count += 1
            continue
        except ValueError as e:
            logger.warning(f"Record {i+1} skipped. Record: {raw_record}. Error: {e}")
            failed_count += 1
            continue

    logger.info(
        f"Finished transformation. Processed: {validated_count}. "
        f"Failed: {failed_count}."
    )
