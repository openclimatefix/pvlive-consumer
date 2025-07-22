"""
Extract Historical GSP Data to Zarr Format

Migrated from archived nowcasting_dataset repository
Author: Peter Dudfield (Original), Your Name (Migration)
Date: 2025-01-22

Pulls raw PV GSP data from Sheffield Solar API and saves to compressed Zarr format.
Data volume: ~1MB per month of data
"""
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List

import numcodecs
import pandas as pd
import pytz
import xarray as xr
import yaml
from pvlive_api import PVLive
from sqlalchemy.orm import Session

# Use existing logging setup from pvlive-consumer
logger = logging.getLogger(__name__)

class HistoricalGSPExtractor:
    """Extract and process historical GSP data from PVLive API"""
    
    def __init__(self, pvlive_domain: str = "api.pvlive.uk"):
        """
        Initialize the extractor
        
        Args:
            pvlive_domain: PVLive API domain URL
        """
        self.pvlive = PVLive(domain_url=pvlive_domain)
        # Use the same ignored GSP IDs as the main app
        self.ignore_gsp_ids = [5, 17, 53, 75, 139, 140, 143, 157, 163, 225, 310]
    
    def extract_gsp_data(
        self, 
        start: datetime, 
        end: datetime,
        output_path: Path,
        gsp_ids: Optional[List[int]] = None,
        normalize_data: bool = False
    ) -> None:
        """
        Extract GSP data and save to Zarr format
        
        Args:
            start: Start datetime (UTC)
            end: End datetime (UTC) 
            output_path: Path to save the zarr file
            gsp_ids: List of GSP IDs to extract (if None, extracts all)
            normalize_data: Whether to normalize the data
        """
        logger.info(f"Extracting GSP data from {start} to {end}")
        
        # Get GSP data
        data_df = self._load_pv_gsp_raw_data(start, end, gsp_ids, normalize_data)
        
        if data_df.empty:
            logger.warning("No data retrieved from PVLive API")
            return
        
        # Process and convert to xarray
        xarray_dataset = self._process_dataframe_to_xarray(data_df)
        
        # Save to zarr with compression
        self._save_to_zarr(xarray_dataset, output_path, start, end)
        
        logger.info(f"Successfully saved {len(data_df)} records to {output_path}")
    
    def _load_pv_gsp_raw_data(
        self, 
        start: datetime, 
        end: datetime, 
        gsp_ids: Optional[List[int]],
        normalize_data: bool
    ) -> pd.DataFrame:
        """Load raw GSP data from PVLive API"""
        
        if gsp_ids is None:
            # Use all GSP IDs except ignored ones
            gsp_ids = [i for i in range(0, 343) if i not in self.ignore_gsp_ids]
        
        all_data = []
        
        for gsp_id in gsp_ids:
            try:
                logger.debug(f"Fetching data for GSP {gsp_id}")
                
                gsp_data = self.pvlive.between(
                    start=start,
                    end=end,
                    entity_type="gsp",
                    entity_id=gsp_id,
                    dataframe=True,
                    extra_fields="installedcapacity_mwp,capacity_mwp,updated_gmt",
                )
                
                if not gsp_data.empty:
                    gsp_data['gsp_id'] = gsp_id
                    all_data.append(gsp_data)
                    
            except Exception as e:
                logger.warning(f"Failed to fetch data for GSP {gsp_id}: {e}")
                continue
        
        if not all_data:
            return pd.DataFrame()
        
        combined_df = pd.concat(all_data, ignore_index=True)
        logger.info(f"Retrieved data for {len(combined_df)} records across {len(all_data)} GSPs")
        
        return combined_df
    
    def _process_dataframe_to_xarray(self, data_df: pd.DataFrame) -> xr.Dataset:
        """Convert dataframe to xarray dataset with proper structure"""
        
        logger.debug("Converting DataFrame to xarray Dataset")
        
        # Pivot data to get datetime x gsp_id matrices
        data_generation_df = data_df.pivot(
            index="datetime_gmt", columns="gsp_id", values="generation_mw"
        )
        data_installedcapacity_df = data_df.pivot(
            index="datetime_gmt", columns="gsp_id", values="installedcapacity_mwp"
        )
        data_capacity_df = data_df.pivot(
            index="datetime_gmt", columns="gsp_id", values="capacity_mwp"
        )
        data_updated_gmt_df = data_df.pivot(
            index="datetime_gmt", columns="gsp_id", values="updated_gmt"
        )
        
        # Create xarray dataset
        dataset = xr.Dataset(
            data_vars={
                "generation_mw": (("datetime_gmt", "gsp_id"), data_generation_df),
                "installedcapacity_mwp": (("datetime_gmt", "gsp_id"), data_installedcapacity_df),
                "capacity_mwp": (("datetime_gmt", "gsp_id"), data_capacity_df),
                "updated_gmt": (("datetime_gmt", "gsp_id"), data_updated_gmt_df),
            },
            coords={
                "datetime_gmt": data_generation_df.index, 
                "gsp_id": data_generation_df.columns
            },
            attrs={
                "title": "Historical GSP PV Generation Data",
                "source": "Sheffield Solar PVLive API",
                "created_by": "pvlive-consumer historical extractor"
            }
        )
        
        return dataset
    
    def _save_to_zarr(
        self, 
        dataset: xr.Dataset, 
        output_path: Path,
        start: datetime,
        end: datetime
    ) -> None:
        """Save xarray dataset to zarr with compression and metadata"""
        
        # Create output directory
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Configure compression
        encoding = {
            var: {"compressor": numcodecs.Blosc(cname="zstd", clevel=5)}
            for var in dataset.data_vars
        }
        
        # Save to zarr
        dataset.to_zarr(output_path, mode="w", encoding=encoding)
        
        # Save configuration metadata
        config = {
            "extraction_config": {
                "start_date": start.isoformat(),
                "end_date": end.isoformat(), 
                "created_at": datetime.now(pytz.UTC).isoformat(),
                "n_gsps": len(dataset.gsp_id),
                "n_timestamps": len(dataset.datetime_gmt),
                "data_source": "Sheffield Solar PVLive API",
                "extractor_version": "2.0"
            }
        }
        
        config_path = output_path.parent / "extraction_metadata.yaml"
        with open(config_path, "w") as f:
            yaml.dump(config, f, default_flow_style=False)
        
        logger.info(f"Saved metadata to {config_path}")


def main():
    """Main execution function for standalone use"""
    
    # Configuration - adjust these as needed
    start = datetime(2016, 1, 1, tzinfo=pytz.UTC)
    end = datetime(2021, 10, 1, tzinfo=pytz.UTC) 
    output_path = Path("./data/historical_gsp_data.zarr")
    
    # Initialize and run extractor
    extractor = HistoricalGSPExtractor()
    extractor.extract_gsp_data(
        start=start,
        end=end, 
        output_path=output_path,
        normalize_data=False
    )


if __name__ == "__main__":
    main()
