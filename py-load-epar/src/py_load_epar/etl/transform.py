import hashlib
import logging
from typing import Any, Dict, Iterator, List, Tuple

from pydantic import ValidationError

from py_load_epar.models import EparIndex, EparSubstanceLink
from py_load_epar.spor_api.client import SporApiClient

logger = logging.getLogger(__name__)


def transform_and_validate(
    raw_records: Iterator[Dict[str, Any]], spor_client: SporApiClient
) -> Iterator[Tuple[EparIndex, List[EparSubstanceLink]]]:
    """
    Transforms raw data, validates it, and enriches it with SPOR API data.

    - Validates raw data against the EparIndex Pydantic model.
    - Generates a stable `epar_id`.
    - Enriches organisation data using SPOR OMS.
    - Enriches substance data using SPOR SMS and creates link table records.
    - Records that fail validation are logged and skipped (quarantined).

    Args:
        raw_records: An iterator yielding dictionaries of raw EPAR data.
        spor_client: An instance of the SporApiClient for enrichment.

    Yields:
        A tuple containing the enriched EparIndex Pydantic model instance
        and a list of EparSubstanceLink instances for that EPAR.
    """
    logger.info("Starting data transformation, validation, and enrichment.")
    validated_count = 0
    failed_count = 0

    for i, raw_record in enumerate(raw_records):
        try:
            # TODO: The source data does not have a clear, stable unique ID.
            # We are generating a synthetic one using a hash. This should be
            # replaced with a proper source ID field if one becomes available.
            med_name = raw_record.get("medicine_name", "")
            mah_name = raw_record.get("marketing_authorization_holder_raw", "")
            if not med_name or not mah_name:
                raise ValueError("Missing medicine_name or marketing_authorization_holder_raw")

            id_string = f"{med_name}-{mah_name}"
            raw_record["epar_id"] = hashlib.sha1(id_string.encode()).hexdigest()[:20]


            # 1. Validate the base record
            validated_model = EparIndex.model_validate(raw_record)
            substance_links = []

            # 2. Enrich Organisation (MAH)
            if validated_model.marketing_authorization_holder_raw:
                org = spor_client.search_organisation(
                    validated_model.marketing_authorization_holder_raw
                )
                if org:
                    validated_model.mah_oms_id = org.org_id
                    logger.debug(f"Enriched MAH '{org.name}' with OMS ID {org.org_id}")

            # 3. Enrich Substances
            if validated_model.active_substance_raw:
                # Simple split, can be improved with better parsing
                substance_names = [s.strip() for s in validated_model.active_substance_raw.split(',')]
                for sub_name in substance_names:
                    if not sub_name:
                        continue
                    substance = spor_client.search_substance(sub_name)
                    if substance:
                        link = EparSubstanceLink(
                            epar_id=validated_model.epar_id,
                            spor_substance_id=substance.sms_id,
                        )
                        substance_links.append(link)
                        logger.debug(f"Enriched substance '{sub_name}' with SMS ID {substance.sms_id}")


            yield validated_model, substance_links
            validated_count += 1

        except ValidationError as e:
            logger.warning(
                f"Record {i+1} failed validation and will be quarantined. "
                f"Record: {raw_record}. Error: {e}"
            )
            failed_count += 1
            # In a real system, this failed record would be sent to a
            # dead-letter queue or error table.
            continue
        except ValueError as e:
            logger.warning(
                f"Record {i+1} could not be processed and will be skipped. "
                f"Record: {raw_record}. Error: {e}"
            )
            failed_count += 1
            continue


    logger.info(
        f"Finished transformation. "
        f"Successfully processed: {validated_count}. Failed/Skipped: {failed_count}."
    )
