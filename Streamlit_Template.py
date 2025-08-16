import pandas as pd
import numpy as np
import os
import json
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from scipy.stats import norm
import math
import time
from datetime import datetime
import threading
from sklearn.linear_model import LinearRegression
from streamlit_extras.stylable_container import stylable_container
import warnings
import io
from io import BytesIO
import streamlit as st
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
import gspread
from google.oauth2.service_account import Credentials

warnings.filterwarnings('ignore')

# Always reference files relative to the script's directory
try:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
except NameError:
    BASE_DIR = os.getcwd()

print(f"[DEBUG] BASE_DIR set to: {BASE_DIR}")


try:
    GOOGLE_SHEETS_AVAILABLE = True
except ImportError:
    GOOGLE_SHEETS_AVAILABLE = False

class GoogleSheetsConnector:
    def __init__(self, credentials_file='credentials.json'):
        if not GOOGLE_SHEETS_AVAILABLE:
            raise ImportError("Google Sheets packages not available")

        try:
            print("Connecting to Google Sheets...")
            scopes = [
                'https://www.googleapis.com/auth/spreadsheets.readonly',
                'https://www.googleapis.com/auth/drive.readonly'
            ]
            credentials = Credentials.from_service_account_file(credentials_file, scopes=scopes)
            self.gc = gspread.authorize(credentials)
            print("Google Sheets connection established")
        except Exception as e:
            print(f"Error connecting to Google Sheets: {e}")
            raise

    def get_inventory_data(self, spreadsheet_url):
        """
        Extract inventory data from Google Sheets
        Column B: SKU
        Column AC: Inventory total
        """
        try:
            print("📦 Extracting inventory data from Google Sheets...")
            spreadsheet = self.gc.open_by_url(spreadsheet_url)
            
            # Get the first worksheet (or specify the worksheet name if needed)
            worksheet = spreadsheet.get_worksheet(0)  # First sheet
            
            # Get all values
            all_values = worksheet.get_all_values()
            
            if not all_values:
                print("❌ No data found in inventory sheet")
                return {}
            
            # Find column indices (B = index 1, AC = index 28)
            sku_col = 1  # Column B
            inventory_col = 28  # Column AC
            
            inventory_data = {}
            skus_processed = 0
            
            # Process data rows (assuming row 1 is header)
            for row_idx, row in enumerate(all_values[1:], start=2):
                try:
                    # Ensure row has enough columns
                    if len(row) <= max(sku_col, inventory_col):
                        continue
                    
                    # Extract SKU
                    raw_sku = str(row[sku_col]).strip()
                    if not raw_sku or raw_sku.lower() in ['', 'none', 'null', 'n/a']:
                        continue
                    
                    # Clean SKU - multiple formats
                    cleaned_skus = []
                    
                    # Original format
                    cleaned_skus.append(raw_sku)
                    
                    # Remove leading zeros
                    cleaned_skus.append(raw_sku.lstrip('0'))
                    
                    # Add leading zeros if short UPC
                    if raw_sku.isdigit() and len(raw_sku) < 12:
                        cleaned_skus.append(raw_sku.zfill(12))
                    
                    # Remove non-alphanumeric
                    alphanumeric_only = ''.join(c for c in raw_sku if c.isalnum())
                    if alphanumeric_only:
                        cleaned_skus.append(alphanumeric_only)
                    
                    # Extract inventory quantity
                    inventory_value = row[inventory_col] if inventory_col < len(row) else '0'
                    
                    try:
                        inventory_qty = float(str(inventory_value).replace(',', '').strip())
                        if inventory_qty < 0:
                            inventory_qty = 0
                    except:
                        inventory_qty = 0
                    
                    # Store all SKU variations with the same inventory value
                    for sku_variant in cleaned_skus:
                        if sku_variant:
                            inventory_data[sku_variant] = inventory_qty
                            
                    skus_processed += 1
                    
                except Exception as e:
                    print(f"   Warning: Error processing row {row_idx}: {e}")
                    continue
            
            print(f"✅ Extracted inventory for {skus_processed} SKUs")
            print(f"   Total inventory records (including variants): {len(inventory_data)}")
            
            # Show sample data
            if inventory_data:
                print(f"\n🔍 SAMPLE INVENTORY DATA:")
                sample_items = list(inventory_data.items())[:5]
                for sku, qty in sample_items:
                    print(f"   SKU: '{sku}' -> Inventory: {qty}")
            
            return inventory_data
            
        except Exception as e:
            print(f"❌ Error extracting inventory data: {e}")
            import traceback
            traceback.print_exc()
            return {}

    def get_product_data(self, spreadsheet_url):  ##get_data
        try:
            print("Extracting product data from Google Sheets...")
            spreadsheet = self.gc.open_by_url(spreadsheet_url)
            worksheet = spreadsheet.worksheet("1. Finished_Products")

            # Get all values to handle duplicate headers manually
            all_values = worksheet.get_all_values()

            if not all_values:
                print("No data found in the worksheet")
                return {}, {}, {}, {}, {}

            # Get headers and clean them
            headers = all_values[0]

            # Find the columns we need by index - MORE FLEXIBLE MATCHING
            product_name_col = None
            launch_date_col = None
            upc_col = None
            price_col = None
            lead_time_col = None
            category_col = None
            status_col = None

            # Look for our target columns (case insensitive and partial matching)
            for i, header in enumerate(headers):
                header_clean = str(header).strip().lower()

                if any(keyword in header_clean for keyword in ['product name', 'product_name', 'name']):
                    product_name_col = i
                    print(f"✅ Found 'Product Name' in column {i}: {header}")
                elif any(keyword in header_clean for keyword in ['launch date', 'launch_date', 'date']):
                    launch_date_col = i
                    print(f"✅ Found 'Launch Date' in column {i}: {header}")
                elif any(keyword in header_clean for keyword in ['upc', 'sku', 'barcode', 'unit upc']):
                    upc_col = i
                    print(f"✅ Found 'UPC/SKU' in column {i}: {header}")
                elif any(keyword in header_clean for keyword in ['price', 'cost', 'msrp']):
                    price_col = i
                    print(f"✅ Found 'Price' in column {i}: {header}")
                elif any(keyword in header_clean for keyword in ['lead time', 'lead_time', 'leadtime']):
                    lead_time_col = i
                    print(f"✅ Found 'Lead Time' in column {i}: {header}")
                elif any(keyword in header_clean for keyword in ['category', 'product_category', 'product category']):
                    category_col = i
                    print(f"✅ Found 'Category' in column {i}: {header}")
                elif header_clean == 'status':
                    status_col = i
                    print(f"✅ Found 'Status' in column {i}: {header}")


            if upc_col is None:
                print("❌ Could not find UPC/SKU column")
                return {}, {}, {}, {}, {}

            product_info = {}
            lead_times = {}
            launch_dates = {}  # Store launch dates for each SKU
            sku_variations = {}  # Track different formats of the same SKU
            product_category = {}
            product_status = {}

            # Process data rows (skip header row)
            for row_idx, row in enumerate(all_values[1:], start=2):
                try:
                    # Ensure row has enough columns
                    if len(row) <= max(filter(None, [product_name_col, launch_date_col, upc_col, price_col, lead_time_col, category_col, status_col])):
                        continue

                    # Extract UPC/SKU with multiple cleaning approaches
                    raw_upc = str(row[upc_col]).strip() if upc_col < len(row) else ''
                    if not raw_upc or raw_upc.lower() in ['', 'none', 'null', 'n/a']:
                        continue

                    # Clean UPC/SKU - try multiple formats
                    cleaned_upcs = []

                    # Original format
                    cleaned_upcs.append(raw_upc)

                    # Remove leading zeros (common issue)
                    cleaned_upcs.append(raw_upc.lstrip('0'))

                    # Add leading zeros if it looks like a short UPC
                    if raw_upc.isdigit() and len(raw_upc) < 12:
                        cleaned_upcs.append(raw_upc.zfill(12))

                    # Remove all non-alphanumeric characters
                    alphanumeric_only = ''.join(c for c in raw_upc if c.isalnum())
                    if alphanumeric_only:
                        cleaned_upcs.append(alphanumeric_only)

                    # Remove spaces and dashes
                    no_spaces_dashes = raw_upc.replace(' ', '').replace('-', '')
                    if no_spaces_dashes:
                        cleaned_upcs.append(no_spaces_dashes)

                    # Extract other data
                    product_name = str(row[product_name_col]).strip() if product_name_col is not None and product_name_col < len(row) else 'Unknown'
                    launch_date_raw = str(row[launch_date_col]).strip() if launch_date_col is not None and launch_date_col < len(row) else ''
                    price = row[price_col] if price_col is not None and price_col < len(row) else 0
                    lead_time = row[lead_time_col] if lead_time_col is not None and lead_time_col < len(row) else 2
                    category_name = str(row[category_col]).strip() if category_col is not None and category_col < len(row) else 'Unknown'
                    status_name = str(row[status_col]).strip() if status_col is not None and status_col < len(row) else 'Unknown'

                    # Clean and parse launch date
                    launch_date_parsed = None
                    if launch_date_raw and launch_date_raw.lower() not in ['', 'none', 'null', 'n/a']:
                        try:
                            # Try multiple date formats
                            for date_format in ['%m/%d/%Y', '%Y-%m-%d', '%m-%d-%Y', '%d/%m/%Y', '%Y/%m/%d']:
                                try:
                                    launch_date_parsed = pd.to_datetime(launch_date_raw, format=date_format)
                                    break
                                except:
                                    continue

                            # If still not parsed, try pandas auto-detection
                            if launch_date_parsed is None:
                                launch_date_parsed = pd.to_datetime(launch_date_raw, errors='coerce')

                        except Exception as e:
                            print(f"   Warning: Could not parse launch date '{launch_date_raw}' for row {row_idx}")
                            launch_date_parsed = None

                    # Clean and validate data
                    try:
                        price = float(str(price).replace('$', '').replace(',', '')) if price else 0
                    except:
                        price = 0

                    try:
                        lead_time = int(float(str(lead_time))) if lead_time else 2
                    except:
                        lead_time = 2

                    # Store all UPC variations for this product
                    for upc_variant in cleaned_upcs:
                        if upc_variant and upc_variant not in product_info:
                            product_info[upc_variant] = product_name
                            product_category[upc_variant] = category_name
                            product_status[upc_variant] = status_name
                            lead_times[upc_variant] = lead_time
                            launch_dates[upc_variant] = launch_date_parsed  # Store parsed date
                            sku_variations[upc_variant] = {
                                'original': raw_upc,
                                'product_name': product_name,
                                'category_name': category_name,
                                'status_name': status_name,
                                'launch_date': launch_date_parsed,
                                'row': row_idx
                            }

                except Exception as e:
                    print(f"Error processing row {row_idx}: {e}")
                    continue

            print(f"✅ Extracted data for {len(product_info)} product variations")
            print(f"✅ Extracted data for {len(product_category)} product variations")
            print(f"✅ Extracted data for {len(product_status)} product variations")
            print(f"✅ Extracted lead times for {len(lead_times)} product variations")
            print(f"✅ Extracted launch dates for {len([d for d in launch_dates.values() if d is not None])} product variations")

            # Debug: Show some examples of what we extracted
            print(f"\n🔍 SAMPLE EXTRACTED DATA:")
            sample_items = list(product_info.items())[:5]
            for sku, name in sample_items:
                launch_info = launch_dates.get(sku)
                launch_str = launch_info.strftime('%Y-%m-%d') if launch_info else 'No Date'
                print(f"   SKU: '{sku}' -> Product: '{name}' | Launch: {launch_str} | Lead Time: {lead_times.get(sku, 'N/A')}")

            return product_info, lead_times, launch_dates, product_category, product_status

        except Exception as e:
            print(f"❌ Error extracting product data: {e}")
            import traceback
            traceback.print_exc()
            return {}, {}, {}, {}, {}

    def get_amazon_fba_weekly_sales(self, spreadsheet_url):
        try:
            print("📦 Extracting Amazon FBA weekly sales data...")
            spreadsheet = self.gc.open_by_url(spreadsheet_url)
            worksheet = spreadsheet.worksheet("Amazon FBA")

            all_values = worksheet.get_all_values()
            if not all_values:
                print("❌ No data found.")
                return pd.DataFrame()

            headers = all_values[0]
            print(f"🧾 Headers: {headers}")

            sku_col = None
            week_columns = []

            # Identify columns
            for i, header in enumerate(headers):
                header_clean = str(header).strip().lower()

                if 'upc' in header_clean:
                    sku_col = i
                    print(f"✅ Found UPC column at index {i}: {header}")
                    continue

                # Try to parse as date
                try:
                    week_date = pd.to_datetime(header, dayfirst=False)  # mm/dd/yyyy format
                    week_columns.append((i, week_date))
                    print(f"📅 Week column at index {i}: {week_date.strftime('%d/%m/%Y')}")
                except:
                    # Print once for debugging
                    if i >= 4:  # Avoid clutter for known non-date columns
                        print(f"⛔ Skipping non-date column: {header}")
                    continue

            if sku_col is None or not week_columns:
                print("❌ Could not identify necessary columns.")
                return pd.DataFrame()

            # Parse each row
            weekly_sales_data = []
            processed_skus = set()

            for row_idx, row in enumerate(all_values[1:], start=2):
                try:
                    if len(row) <= sku_col:
                        continue

                    raw_sku = str(row[sku_col]).strip()
                    if not raw_sku or raw_sku.lower() in ['none', 'null', 'n/a', '']:
                        continue

                    # Clean SKU
                    cleaned_sku = raw_sku.zfill(12) if raw_sku.isdigit() and len(raw_sku) < 12 else raw_sku
                    processed_skus.add(cleaned_sku)

                    for col_idx, week_date in week_columns:
                        if col_idx >= len(row):
                            continue
                        try:
                            sales = float(row[col_idx]) if row[col_idx].strip() not in ['', '-', 'n/a', 'N/A'] else 0
                        except:
                            sales = 0

                        if sales > 0:
                            weekly_sales_data.append({
                                'SKU': cleaned_sku,
                                'Original_SKU': raw_sku,
                                'Week_Start': week_date,
                                'Sales': sales,
                                'Channel': 'Amazon'
                            })

                except Exception as e:
                    print(f"⚠️ Error at row {row_idx}: {e}")
                    continue

            # Convert to DataFrame
            weekly_df = pd.DataFrame(weekly_sales_data)

            if weekly_df.empty:
                print("⚠️ No valid weekly sales found.")
            else:
                print(f"✅ {len(weekly_df)} records extracted | {weekly_df['SKU'].nunique()} SKUs")
                print(f"   Range: {weekly_df['Week_Start'].min().strftime('%d/%m/%Y')} → {weekly_df['Week_Start'].max().strftime('%d/%m/%Y')}")

            return weekly_df

        except Exception as e:
            print(f"❌ Fatal error during extraction: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()

    def convert_amazon_weekly_to_monthly(self, weekly_df):
        """
        Convert Amazon FBA weekly sales data to monthly format
        using actual week start dates.
        """
        try:
            if weekly_df.empty:
                return pd.DataFrame()

            print(f"Converting Amazon FBA weekly sales to monthly format...")

            # Ensure 'Week_Start' column is datetime
            weekly_df['Week_Start'] = pd.to_datetime(weekly_df['Week_Start'], errors='coerce')

            # Drop rows with invalid dates
            weekly_df = weekly_df.dropna(subset=['Week_Start'])

            # Assign the month (e.g., 2025-01-31, 2025-02-28 etc.) to each week using MonthEnd
            weekly_df['Month'] = weekly_df['Week_Start'] + pd.offsets.MonthEnd(0)

            # Debug print mapping
            mapping = weekly_df[['Week_Start', 'Month']].drop_duplicates().sort_values('Week_Start')
            print("Week to Month mapping:")
            for _, row in mapping.head(10).iterrows():
                print(f"   Week of {row['Week_Start'].strftime('%d/%m/%Y')} → Month End: {row['Month'].strftime('%d/%m/%Y')}")

            # Group and aggregate
            monthly_sales = weekly_df.groupby(['SKU', 'Month'])['Sales'].sum().reset_index()
            monthly_sales['Channel'] = 'Amazon'
            monthly_sales = monthly_sales.rename(columns={'Month': 'Date'})

            print(f"✅ Converted to {len(monthly_sales)} monthly records")
            print(f"   Date range: {monthly_sales['Date'].min().strftime('%d/%m/%Y')} to {monthly_sales['Date'].max().strftime('%d/%m/%Y')}")
            print(f"   SKUs covered: {monthly_sales['SKU'].nunique()}")

            # Sample output
            for _, row in monthly_sales.head(3).iterrows():
                print(f"   SKU {row['SKU']} | {row['Date'].strftime('%b %Y')} | Sales: {row['Sales']}")

            return monthly_sales

        except Exception as e:
            print(f"❌ Error converting Amazon FBA weekly to monthly: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()

    def extend_historical_data_with_amazon_weekly(self, historical_data, amazon_weekly_monthly_data):
        """
        Extend historical sales data with Amazon FBA weekly data
        """
        try:
            if amazon_weekly_monthly_data.empty:
                print("No Amazon FBA weekly data to extend historical data")
                return historical_data

            print("Extending historical data with Amazon FBA weekly sales...")

            # Get the latest date in historical data
            historical_data['Date'] = pd.to_datetime(historical_data['Date'])
            latest_historical_date = historical_data['Date'].max()

            print(f"Latest historical date: {latest_historical_date}")
            print(f"Amazon FBA weekly data starts: {amazon_weekly_monthly_data['Date'].min()}")
            print(f"Amazon FBA weekly data ends: {amazon_weekly_monthly_data['Date'].max()}")

            # Filter Amazon weekly data to only include dates after historical data
            new_amazon_data = amazon_weekly_monthly_data[amazon_weekly_monthly_data['Date'] > latest_historical_date].copy()

            if new_amazon_data.empty:
                print("No new Amazon FBA weekly data after historical cutoff")
                return historical_data

            print(f"Adding {len(new_amazon_data)} new Amazon FBA monthly records")

            # Amazon FBA data is already in the right format - just add it
            if not new_amazon_data.empty:
                # Combine with historical data
                combined_data = pd.concat([historical_data, new_amazon_data], ignore_index=True)
                combined_data = combined_data.sort_values(['SKU', 'Channel', 'Date'])

                print(f"✅ Extended historical data with {len(new_amazon_data)} new Amazon records")
                print(f"   Total records: {len(combined_data)} (was {len(historical_data)})")
                print(f"   New date range: {combined_data['Date'].min()} to {combined_data['Date'].max()}")

                # Show sample of extended data
                amazon_sample = new_amazon_data.head(3)
                for _, row in amazon_sample.iterrows():
                    print(f"   Added Amazon: {row['SKU']}, {row['Date'].strftime('%b %Y')}, Sales: {row['Sales']}")

                return combined_data
            else:
                return historical_data

        except Exception as e:
            print(f"❌ Error extending historical data with Amazon FBA: {e}")
            import traceback
            traceback.print_exc()
            return historical_data
        
    ### For Shopify Data Seprately

    def get_shopify_main_weekly_sales(self, spreadsheet_url):
        try:
            print("📦 Extracting Shopify Main weekly sales data...")
            spreadsheet = self.gc.open_by_url(spreadsheet_url)
            worksheet = spreadsheet.worksheet("Shopify Main")

            all_values = worksheet.get_all_values()
            if not all_values:
                print("❌ No data found.")
                return pd.DataFrame()

            headers = all_values[0]
            print(f"🧾 Headers: {headers}")

            sku_col = None
            week_columns = []

            # Identify columns
            for i, header in enumerate(headers):
                header_clean = str(header).strip().lower()

                if 'upc' in header_clean:
                    sku_col = i
                    print(f"✅ Found UPC column at index {i}: {header}")
                    continue

                # Try to parse as date
                try:
                    week_date = pd.to_datetime(header, dayfirst=False)  # mm/dd/yyyy format
                    week_columns.append((i, week_date))
                    print(f"📅 Week column at index {i}: {week_date.strftime('%d/%m/%Y')}")
                except:
                    # Print once for debugging
                    if i >= 4:  # Avoid clutter for known non-date columns
                        print(f"⛔ Skipping non-date column: {header}")
                    continue

            if sku_col is None or not week_columns:
                print("❌ Could not identify necessary columns.")
                return pd.DataFrame()

            # Parse each row
            weekly_sales_data = []
            processed_skus = set()

            for row_idx, row in enumerate(all_values[1:], start=2):
                try:
                    if len(row) <= sku_col:
                        continue

                    raw_sku = str(row[sku_col]).strip()
                    if not raw_sku or raw_sku.lower() in ['none', 'null', 'n/a', '']:
                        continue

                    # Clean SKU
                    cleaned_sku = raw_sku.zfill(12) if raw_sku.isdigit() and len(raw_sku) < 12 else raw_sku
                    processed_skus.add(cleaned_sku)

                    for col_idx, week_date in week_columns:
                        if col_idx >= len(row):
                            continue
                        try:
                            sales = float(row[col_idx]) if row[col_idx].strip() not in ['', '-', 'n/a', 'N/A'] else 0
                        except:
                            sales = 0

                        if sales > 0:
                            weekly_sales_data.append({
                                'SKU': cleaned_sku,
                                'Original_SKU': raw_sku,
                                'Week_Start': week_date,
                                'Sales': sales,
                                'Channel': 'Shopify'
                            })

                except Exception as e:
                    print(f"⚠️ Error at row {row_idx}: {e}")
                    continue

            # Convert to DataFrame
            weekly_df2 = pd.DataFrame(weekly_sales_data)

            if weekly_df2.empty:
                print("⚠️ No valid weekly sales found.")
            else:
                print(f"✅ {len(weekly_df2)} records extracted | {weekly_df2['SKU'].nunique()} SKUs")
                print(f"   Range: {weekly_df2['Week_Start'].min().strftime('%d/%m/%Y')} → {weekly_df2['Week_Start'].max().strftime('%d/%m/%Y')}")

            return weekly_df2

        except Exception as e:
            print(f"❌ Fatal error during extraction: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()

    def convert_shopify_weekly_to_monthly(self, weekly_df2):
        """
        Convert Shopify Main weekly sales data to monthly format
        using actual week start dates.
        """
        try:
            if weekly_df2.empty:
                return pd.DataFrame()

            print(f"Converting Shopify Main weekly sales to monthly format...")

            # Ensure 'Week_Start' column is datetime
            weekly_df2['Week_Start'] = pd.to_datetime(weekly_df2['Week_Start'], errors='coerce')

            # Drop rows with invalid dates
            weekly_df2 = weekly_df2.dropna(subset=['Week_Start'])

            # Assign the month (e.g., 2025-01-31, 2025-02-28 etc.) to each week using MonthEnd
            weekly_df2['Month'] = weekly_df2['Week_Start'] + pd.offsets.MonthEnd(0)

            # Debug print mapping
            mapping = weekly_df2[['Week_Start', 'Month']].drop_duplicates().sort_values('Week_Start')
            print("Week to Month mapping:")
            for _, row in mapping.head(10).iterrows():
                print(f"   Week of {row['Week_Start'].strftime('%d/%m/%Y')} → Month End: {row['Month'].strftime('%d/%m/%Y')}")

            # Group and aggregate
            monthly_sales2 = weekly_df2.groupby(['SKU', 'Month'])['Sales'].sum().reset_index()
            monthly_sales2['Channel'] = 'Shopify'
            monthly_sales2 = monthly_sales2.rename(columns={'Month': 'Date'})

            print(f"✅ Converted to {len(monthly_sales2)} monthly records")
            print(f"   Date range: {monthly_sales2['Date'].min().strftime('%d/%m/%Y')} to {monthly_sales2['Date'].max().strftime('%d/%m/%Y')}")
            print(f"   SKUs covered: {monthly_sales2['SKU'].nunique()}")

            # Sample output
            for _, row in monthly_sales2.head(3).iterrows():
                print(f"   SKU {row['SKU']} | {row['Date'].strftime('%b %Y')} | Sales: {row['Sales']}")

            return monthly_sales2

        except Exception as e:
            print(f"❌ Error converting Shopify Main weekly to monthly: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()
    
    def extend_historical_data_with_shopify_weekly(self, historical_data, shopify_weekly_monthly_data):
        """
        Extend historical sales data with Shopify Main weekly data
        """
        try:
            if shopify_weekly_monthly_data.empty:
                print("No Shopify Main weekly data to extend historical data")
                return historical_data

            print("Extending historical data with Shopify weekly sales...")

            # Get the latest date in historical data
            historical_data['Date'] = pd.to_datetime(historical_data['Date'])
            latest_historical_date = historical_data['Date'].max()

            print(f"Latest historical date: {latest_historical_date}")
            print(f"Shopify Main weekly data starts: {shopify_weekly_monthly_data['Date'].min()}")
            print(f"Shopify Main weekly data ends: {shopify_weekly_monthly_data['Date'].max()}")

            # Filter Shopify weekly data to only include dates after historical data
            new_shopify_data = shopify_weekly_monthly_data[shopify_weekly_monthly_data['Date'] > latest_historical_date].copy()

            if new_shopify_data.empty:
                print("No new Shopify Main weekly data after historical cutoff")
                return historical_data

            print(f"Adding {len(new_shopify_data)} new Shopify Main monthly records")

            # Shopify Main data is already in the right format - just add it
            if not new_shopify_data.empty:
                # Combine with historical data
                combined_data = pd.concat([historical_data, new_shopify_data], ignore_index=True)
                combined_data = combined_data.sort_values(['SKU', 'Channel', 'Date'])

                print(f"✅ Extended historical data with {len(new_shopify_data)} new Shopify records")
                print(f"   Total records: {len(combined_data)} (was {len(historical_data)})")
                print(f"   New date range: {combined_data['Date'].min()} to {combined_data['Date'].max()}")

                # Show sample of extended data
                shopify_sample = new_shopify_data.head(3)
                for _, row in shopify_sample.iterrows():
                    print(f"   Added Shopify: {row['SKU']}, {row['Date'].strftime('%b %Y')}, Sales: {row['Sales']}")

                return combined_data
            else:
                return historical_data

        except Exception as e:
            print(f"❌ Error extending historical data with Shopify Main: {e}")
            import traceback
            traceback.print_exc()
            return historical_data    

### Just ADDED Shopify Faire Tab

    def get_shopify_faire_weekly_sales(self, spreadsheet_url):
        try:
            print("📦 Extracting Shopify Faire weekly sales data...")
            spreadsheet = self.gc.open_by_url(spreadsheet_url)
            worksheet = spreadsheet.worksheet("Shopify Faire")

            all_values = worksheet.get_all_values()
            if not all_values:
                print("❌ No data found.")
                return pd.DataFrame()

            headers = all_values[0]
            print(f"🧾 Headers: {headers}")

            sku_col = None
            week_columns = []

            # Identify columns
            for i, header in enumerate(headers):
                header_clean = str(header).strip().lower()

                if 'upc' in header_clean:
                    sku_col = i
                    print(f"✅ Found UPC column at index {i}: {header}")
                    continue

                # Try to parse as date
                try:
                    week_date = pd.to_datetime(header, dayfirst=False)  # mm/dd/yyyy format
                    week_columns.append((i, week_date))
                    print(f"📅 Week column at index {i}: {week_date.strftime('%d/%m/%Y')}")
                except:
                    # Print once for debugging
                    if i >= 4:  # Avoid clutter for known non-date columns
                        print(f"⛔ Skipping non-date column: {header}")
                    continue

            if sku_col is None or not week_columns:
                print("❌ Could not identify necessary columns.")
                return pd.DataFrame()

            # Parse each row
            weekly_sales_data = []
            processed_skus = set()

            for row_idx, row in enumerate(all_values[1:], start=2):
                try:
                    if len(row) <= sku_col:
                        continue

                    raw_sku = str(row[sku_col]).strip()
                    if not raw_sku or raw_sku.lower() in ['none', 'null', 'n/a', '']:
                        continue

                    # Clean SKU
                    cleaned_sku = raw_sku.zfill(12) if raw_sku.isdigit() and len(raw_sku) < 12 else raw_sku
                    processed_skus.add(cleaned_sku)

                    for col_idx, week_date in week_columns:
                        if col_idx >= len(row):
                            continue
                        try:
                            sales = float(row[col_idx]) if row[col_idx].strip() not in ['', '-', 'n/a', 'N/A'] else 0
                        except:
                            sales = 0

                        if sales > 0:
                            weekly_sales_data.append({
                                'SKU': cleaned_sku,
                                'Original_SKU': raw_sku,
                                'Week_Start': week_date,
                                'Sales': sales,
                                'Channel': 'Shopify Faire'
                            })

                except Exception as e:
                    print(f"⚠️ Error at row {row_idx}: {e}")
                    continue

            # Convert to DataFrame
            weekly_df3 = pd.DataFrame(weekly_sales_data)

            if weekly_df3.empty:
                print("⚠️ No valid weekly sales found.")
            else:
                print(f"✅ {len(weekly_df3)} records extracted | {weekly_df3['SKU'].nunique()} SKUs")
                print(f"   Range: {weekly_df3['Week_Start'].min().strftime('%d/%m/%Y')} → {weekly_df3['Week_Start'].max().strftime('%d/%m/%Y')}")

            return weekly_df3

        except Exception as e:
            print(f"❌ Fatal error during extraction: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()

    def convert_shopify_faire_weekly_to_monthly(self, weekly_df3):
        """
        Convert Shopify Faire weekly sales data to monthly format
        using actual week start dates.
        """
        try:
            if weekly_df3.empty:
                return pd.DataFrame()

            print(f"Converting Shopify Faire weekly sales to monthly format...")

            # Ensure 'Week_Start' column is datetime
            weekly_df3['Week_Start'] = pd.to_datetime(weekly_df3['Week_Start'], errors='coerce')

            # Drop rows with invalid dates
            weekly_df3 = weekly_df3.dropna(subset=['Week_Start'])

            # Assign the month (e.g., 2025-01-31, 2025-02-28 etc.) to each week using MonthEnd
            weekly_df3['Month'] = weekly_df3['Week_Start'] + pd.offsets.MonthEnd(0)

            # Debug print mapping
            mapping = weekly_df3[['Week_Start', 'Month']].drop_duplicates().sort_values('Week_Start')
            print("Week to Month mapping:")
            for _, row in mapping.head(10).iterrows():
                print(f"   Week of {row['Week_Start'].strftime('%d/%m/%Y')} → Month End: {row['Month'].strftime('%d/%m/%Y')}")

            # Group and aggregate
            monthly_sales3 = weekly_df3.groupby(['SKU', 'Month'])['Sales'].sum().reset_index()
            monthly_sales3['Channel'] = 'Shopify Faire'
            monthly_sales3 = monthly_sales3.rename(columns={'Month': 'Date'})

            print(f"✅ Converted to {len(monthly_sales3)} monthly records")
            print(f"   Date range: {monthly_sales3['Date'].min().strftime('%d/%m/%Y')} to {monthly_sales3['Date'].max().strftime('%d/%m/%Y')}")
            print(f"   SKUs covered: {monthly_sales3['SKU'].nunique()}")

            # Sample output
            for _, row in monthly_sales3.head(3).iterrows():
                print(f"   SKU {row['SKU']} | {row['Date'].strftime('%b %Y')} | Sales: {row['Sales']}")

            return monthly_sales3

        except Exception as e:
            print(f"❌ Error converting Shopify Faire weekly to monthly: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()

    def extend_historical_data_with_shopify_faire_weekly(self, historical_data, shopify_faire_weekly_monthly_data):
        """
        Extend historical sales data with Shopify Faire weekly data
        """
        try:
            if shopify_faire_weekly_monthly_data.empty:
                print("No Shopify Faire weekly data to extend historical data")
                return historical_data

            print("Extending historical data with Shopify Faire weekly sales...")

            # Get the latest date in historical data
            historical_data['Date'] = pd.to_datetime(historical_data['Date'])
            latest_historical_date = historical_data['Date'].max()

            print(f"Latest historical date: {latest_historical_date}")
            print(f"Shopify Faire weekly data starts: {shopify_faire_weekly_monthly_data['Date'].min()}")
            print(f"Shopify Faire weekly data ends: {shopify_faire_weekly_monthly_data['Date'].max()}")

            # Filter Shopify Faire weekly data to only include dates after historical data
            new_shopify_faire_data = shopify_faire_weekly_monthly_data[shopify_faire_weekly_monthly_data['Date'] > latest_historical_date].copy()

            if new_shopify_faire_data.empty:
                print("No new Shopify Faire weekly data after historical cutoff")
                return historical_data

            print(f"Adding {len(new_shopify_faire_data)} new Shopify Faire monthly records")

            # Shopify Faire data is already in the right format - just add it
            if not new_shopify_faire_data.empty:
                # Combine with historical data
                combined_data = pd.concat([historical_data, new_shopify_faire_data], ignore_index=True)
                combined_data = combined_data.sort_values(['SKU', 'Channel', 'Date'])

                print(f"✅ Extended historical data with {len(new_shopify_faire_data)} new Shopify records")
                print(f"   Total records: {len(combined_data)} (was {len(historical_data)})")
                print(f"   New date range: {combined_data['Date'].min()} to {combined_data['Date'].max()}")

                # Show sample of extended data
                shopify_faire_sample = new_shopify_faire_data.head(3)
                for _, row in shopify_faire_sample.iterrows():
                    print(f"   Added Shopify Faire: {row['SKU']}, {row['Date'].strftime('%b %Y')}, Sales: {row['Sales']}")

                return combined_data
            else:
                return historical_data

        except Exception as e:
            print(f"❌ Error extending historical data with Shopify Faire: {e}")
            import traceback
            traceback.print_exc()
            return historical_data     

        ### AMAZON FBM TAB   
        
    def get_amazon_fbm_weekly_sales(self, spreadsheet_url):
        try:
            print("📦 Extracting Amazon FBM weekly sales data...")
            spreadsheet = self.gc.open_by_url(spreadsheet_url)
            worksheet = spreadsheet.worksheet("Amazon FBM")

            all_values = worksheet.get_all_values()
            if not all_values:
                print("❌ No data found.")
                return pd.DataFrame()

            headers = all_values[0]
            print(f"🧾 Headers: {headers}")

            sku_col = None
            week_columns = []

            # Identify columns
            for i, header in enumerate(headers):
                header_clean = str(header).strip().lower()

                if 'upc' in header_clean:
                    sku_col = i
                    print(f"✅ Found UPC column at index {i}: {header}")
                    continue

                # Try to parse as date
                try:
                    week_date = pd.to_datetime(header, dayfirst=False)  # mm/dd/yyyy format
                    week_columns.append((i, week_date))
                    print(f"📅 Week column at index {i}: {week_date.strftime('%d/%m/%Y')}")
                except:
                    # Print once for debugging
                    if i >= 4:  # Avoid clutter for known non-date columns
                        print(f"⛔ Skipping non-date column: {header}")
                    continue

            if sku_col is None or not week_columns:
                print("❌ Could not identify necessary columns.")
                return pd.DataFrame()

            # Parse each row
            weekly_sales_data = []
            processed_skus = set()

            for row_idx, row in enumerate(all_values[1:], start=2):
                try:
                    if len(row) <= sku_col:
                        continue

                    raw_sku = str(row[sku_col]).strip()
                    if not raw_sku or raw_sku.lower() in ['none', 'null', 'n/a', '']:
                        continue

                    # Clean SKU
                    cleaned_sku = raw_sku.zfill(12) if raw_sku.isdigit() and len(raw_sku) < 12 else raw_sku
                    processed_skus.add(cleaned_sku)

                    for col_idx, week_date in week_columns:
                        if col_idx >= len(row):
                            continue
                        try:
                            sales = float(row[col_idx]) if row[col_idx].strip() not in ['', '-', 'n/a', 'N/A'] else 0
                        except:
                            sales = 0

                        if sales > 0:
                            weekly_sales_data.append({
                                'SKU': cleaned_sku,
                                'Original_SKU': raw_sku,
                                'Week_Start': week_date,
                                'Sales': sales,
                                'Channel': 'Amazonfbm'
                            })

                except Exception as e:
                    print(f"⚠️ Error at row {row_idx}: {e}")
                    continue

            # Convert to DataFrame
            weekly_df4 = pd.DataFrame(weekly_sales_data)

            if weekly_df4.empty:
                print("⚠️ No valid weekly sales found.")
            else:
                print(f"✅ {len(weekly_df4)} records extracted | {weekly_df4['SKU'].nunique()} SKUs")
                print(f"   Range: {weekly_df4['Week_Start'].min().strftime('%d/%m/%Y')} → {weekly_df4['Week_Start'].max().strftime('%d/%m/%Y')}")

            return weekly_df4

        except Exception as e:
            print(f"❌ Fatal error during extraction: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()

    def convert_amazon_fbm_weekly_to_monthly(self, weekly_df4):
        """
        Convert Amazon FBM weekly sales data to monthly format
        using actual week start dates.
        """
        try:
            if weekly_df4.empty:
                return pd.DataFrame()

            print(f"Converting Amazon FBM weekly sales to monthly format...")

            # Ensure 'Week_Start' column is datetime
            weekly_df4['Week_Start'] = pd.to_datetime(weekly_df4['Week_Start'], errors='coerce')

            # Drop rows with invalid dates
            weekly_df4 = weekly_df4.dropna(subset=['Week_Start'])

            # Assign the month (e.g., 2025-01-31, 2025-02-28 etc.) to each week using MonthEnd
            weekly_df4['Month'] = weekly_df4['Week_Start'] + pd.offsets.MonthEnd(0)

            # Debug print mapping
            mapping = weekly_df4[['Week_Start', 'Month']].drop_duplicates().sort_values('Week_Start')
            print("Week to Month mapping:")
            for _, row in mapping.head(10).iterrows():
                print(f"   Week of {row['Week_Start'].strftime('%d/%m/%Y')} → Month End: {row['Month'].strftime('%d/%m/%Y')}")

            # Group and aggregate
            monthly_sales4 = weekly_df4.groupby(['SKU', 'Month'])['Sales'].sum().reset_index()
            monthly_sales4['Channel'] = 'Amazonfbm'
            monthly_sales4 = monthly_sales4.rename(columns={'Month': 'Date'})

            print(f"✅ Converted to {len(monthly_sales4)} monthly records")
            print(f"   Date range: {monthly_sales4['Date'].min().strftime('%d/%m/%Y')} to {monthly_sales4['Date'].max().strftime('%d/%m/%Y')}")
            print(f"   SKUs covered: {monthly_sales4['SKU'].nunique()}")

            # Sample output
            for _, row in monthly_sales4.head(3).iterrows():
                print(f"   SKU {row['SKU']} | {row['Date'].strftime('%b %Y')} | Sales: {row['Sales']}")

            return monthly_sales4

        except Exception as e:
            print(f"❌ Error converting Amazon FBM weekly to monthly: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()

    def extend_historical_data_with_amazon_fbm_weekly(self, historical_data, amazon_fbm_weekly_monthly_data):
        """
        Extend historical sales data with Amazon FBM weekly data
        """
        try:
            if amazon_fbm_weekly_monthly_data.empty:
                print("No Amazon FBM weekly data to extend historical data")
                return historical_data

            print("Extending historical data with Amazon FBM weekly sales...")

            # Get the latest date in historical data
            historical_data['Date'] = pd.to_datetime(historical_data['Date'])
            latest_historical_date = historical_data['Date'].max()

            print(f"Latest historical date: {latest_historical_date}")
            print(f"Amazon FBM weekly data starts: {amazon_fbm_weekly_monthly_data['Date'].min()}")
            print(f"Amazon FBM weekly data ends: {amazon_fbm_weekly_monthly_data['Date'].max()}")

            # Filter Amazon FBM weekly data to only include dates after historical data
            new_amazon_fbm_data = amazon_fbm_weekly_monthly_data[amazon_fbm_weekly_monthly_data['Date'] > latest_historical_date].copy()

            if new_amazon_fbm_data.empty:
                print("No new Amazon FBM weekly data after historical cutoff")
                return historical_data

            print(f"Adding {len(new_amazon_fbm_data)} new Amazon FBM monthly records")

            # Amazon FBM data is already in the right format - just add it
            if not new_amazon_fbm_data.empty:
                # Combine with historical data
                combined_data = pd.concat([historical_data, new_amazon_fbm_data], ignore_index=True)
                combined_data = combined_data.sort_values(['SKU', 'Channel', 'Date'])

                print(f"✅ Extended historical data with {len(new_amazon_fbm_data)} new Shopify records")
                print(f"   Total records: {len(combined_data)} (was {len(historical_data)})")
                print(f"   New date range: {combined_data['Date'].min()} to {combined_data['Date'].max()}")

                # Show sample of extended data
                amazon_fbm_sample = new_amazon_fbm_data.head(3)
                for _, row in amazon_fbm_sample.iterrows():
                    print(f"   Added Amazon FBM: {row['SKU']}, {row['Date'].strftime('%b %Y')}, Sales: {row['Sales']}")

                return combined_data
            else:
                return historical_data

        except Exception as e:
            print(f"❌ Error extending historical data with Amazon FBM: {e}")
            import traceback
            traceback.print_exc()
            return historical_data        
        
        ### Walmart FBM Tab

    def get_walmart_fbm_weekly_sales(self, spreadsheet_url):
        try:
            print("📦 Extracting Walmart FBM weekly sales data...")
            spreadsheet = self.gc.open_by_url(spreadsheet_url)
            worksheet = spreadsheet.worksheet("Walmart FBM")

            all_values = worksheet.get_all_values()
            if not all_values:
                print("❌ No data found.")
                return pd.DataFrame()

            headers = all_values[0]
            print(f"🧾 Headers: {headers}")

            sku_col = None
            week_columns = []

            # Identify columns
            for i, header in enumerate(headers):
                header_clean = str(header).strip().lower()

                if 'upc' in header_clean:
                    sku_col = i
                    print(f"✅ Found UPC column at index {i}: {header}")
                    continue

                # Try to parse as date
                try:
                    week_date = pd.to_datetime(header, dayfirst=False)  # mm/dd/yyyy format
                    week_columns.append((i, week_date))
                    print(f"📅 Week column at index {i}: {week_date.strftime('%d/%m/%Y')}")
                except:
                    # Print once for debugging
                    if i >= 4:  # Avoid clutter for known non-date columns
                        print(f"⛔ Skipping non-date column: {header}")
                    continue

            if sku_col is None or not week_columns:
                print("❌ Could not identify necessary columns.")
                return pd.DataFrame()

            # Parse each row
            weekly_sales_data = []
            processed_skus = set()

            for row_idx, row in enumerate(all_values[1:], start=2):
                try:
                    if len(row) <= sku_col:
                        continue

                    raw_sku = str(row[sku_col]).strip()
                    if not raw_sku or raw_sku.lower() in ['none', 'null', 'n/a', '']:
                        continue

                    # Clean SKU
                    cleaned_sku = raw_sku.zfill(12) if raw_sku.isdigit() and len(raw_sku) < 12 else raw_sku
                    processed_skus.add(cleaned_sku)

                    for col_idx, week_date in week_columns:
                        if col_idx >= len(row):
                            continue
                        try:
                            sales = float(row[col_idx]) if row[col_idx].strip() not in ['', '-', 'n/a', 'N/A'] else 0
                        except:
                            sales = 0

                        if sales > 0:
                            weekly_sales_data.append({
                                'SKU': cleaned_sku,
                                'Original_SKU': raw_sku,
                                'Week_Start': week_date,
                                'Sales': sales,
                                'Channel': 'Walmartfbm'
                            })

                except Exception as e:
                    print(f"⚠️ Error at row {row_idx}: {e}")
                    continue

            # Convert to DataFrame
            weekly_df5 = pd.DataFrame(weekly_sales_data)

            if weekly_df5.empty:
                print("⚠️ No valid weekly sales found.")
            else:
                print(f"✅ {len(weekly_df5)} records extracted | {weekly_df5['SKU'].nunique()} SKUs")
                print(f"   Range: {weekly_df5['Week_Start'].min().strftime('%d/%m/%Y')} → {weekly_df5['Week_Start'].max().strftime('%d/%m/%Y')}")

            return weekly_df5

        except Exception as e:
            print(f"❌ Fatal error during extraction: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()

    def convert_walmart_fbm_weekly_to_monthly(self, weekly_df5):
        """
        Convert Walmart FBM weekly sales data to monthly format
        using actual week start dates.
        """
        try:
            if weekly_df5.empty:
                return pd.DataFrame()

            print(f"Converting Walmart FBM weekly sales to monthly format...")

            # Ensure 'Week_Start' column is datetime
            weekly_df5['Week_Start'] = pd.to_datetime(weekly_df5['Week_Start'], errors='coerce')

            # Drop rows with invalid dates
            weekly_df5 = weekly_df5.dropna(subset=['Week_Start'])

            # Assign the month (e.g., 2025-01-31, 2025-02-28 etc.) to each week using MonthEnd
            weekly_df5['Month'] = weekly_df5['Week_Start'] + pd.offsets.MonthEnd(0)

            # Debug print mapping
            mapping = weekly_df5[['Week_Start', 'Month']].drop_duplicates().sort_values('Week_Start')
            print("Week to Month mapping:")
            for _, row in mapping.head(10).iterrows():
                print(f"   Week of {row['Week_Start'].strftime('%d/%m/%Y')} → Month End: {row['Month'].strftime('%d/%m/%Y')}")

            # Group and aggregate
            monthly_sales5 = weekly_df5.groupby(['SKU', 'Month'])['Sales'].sum().reset_index()
            monthly_sales5['Channel'] = 'Walmartfbm'
            monthly_sales5 = monthly_sales5.rename(columns={'Month': 'Date'})

            print(f"✅ Converted to {len(monthly_sales5)} monthly records")
            print(f"   Date range: {monthly_sales5['Date'].min().strftime('%d/%m/%Y')} to {monthly_sales5['Date'].max().strftime('%d/%m/%Y')}")
            print(f"   SKUs covered: {monthly_sales5['SKU'].nunique()}")

            # Sample output
            for _, row in monthly_sales5.head(3).iterrows():
                print(f"   SKU {row['SKU']} | {row['Date'].strftime('%b %Y')} | Sales: {row['Sales']}")

            return monthly_sales5

        except Exception as e:
            print(f"❌ Error converting Walmart FBM weekly to monthly: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()

    def extend_historical_data_with_walmart_fbm_weekly(self, historical_data, walmart_fbm_weekly_monthly_data):
        """
        Extend historical sales data with Walmart FBM weekly data
        """
        try:
            if walmart_fbm_weekly_monthly_data.empty:
                print("No Walmart FBM weekly data to extend historical data")
                return historical_data

            print("Extending historical data with Walmart FBM weekly sales...")

            # Get the latest date in historical data
            historical_data['Date'] = pd.to_datetime(historical_data['Date'])
            latest_historical_date = historical_data['Date'].max()

            print(f"Latest historical date: {latest_historical_date}")
            print(f"Walmart FBM weekly data starts: {walmart_fbm_weekly_monthly_data['Date'].min()}")
            print(f"Walmart FBM weekly data ends: {walmart_fbm_weekly_monthly_data['Date'].max()}")

            # Filter Walmart FBM weekly data to only include dates after historical data
            new_walmart_fbm_data = walmart_fbm_weekly_monthly_data[walmart_fbm_weekly_monthly_data['Date'] > latest_historical_date].copy()

            if new_walmart_fbm_data.empty:
                print("No new Walmart FBM weekly data after historical cutoff")
                return historical_data

            print(f"Adding {len(new_walmart_fbm_data)} new Walmart FBM monthly records")

            # Walmart FBM data is already in the right format - just add it
            if not new_walmart_fbm_data.empty:
                # Combine with historical data
                combined_data = pd.concat([historical_data, new_walmart_fbm_data], ignore_index=True)
                combined_data = combined_data.sort_values(['SKU', 'Channel', 'Date'])

                print(f"✅ Extended historical data with {len(new_walmart_fbm_data)} new Shopify records")
                print(f"   Total records: {len(combined_data)} (was {len(historical_data)})")
                print(f"   New date range: {combined_data['Date'].min()} to {combined_data['Date'].max()}")

                # Show sample of extended data
                walmart_fbm_sample = new_walmart_fbm_data.head(3)
                for _, row in walmart_fbm_sample.iterrows():
                    print(f"   Added Walmart FBM: {row['SKU']}, {row['Date'].strftime('%b %Y')}, Sales: {row['Sales']}")

                return combined_data
            else:
                return historical_data

        except Exception as e:
            print(f"❌ Error extending historical data with Walmart FBM: {e}")
            import traceback
            traceback.print_exc()
            return historical_data        
    

# [REST OF THE CODE REMAINS EXACTLY THE SAME FROM EnhancedForecastingModel CLASS ONWARDS]
# Including all methods in EnhancedForecastingModel and the main() function
# Only the inventory loading part in main() needs to be modified


class EnhancedForecastingModel:
    def __init__(self, historical_data, lead_times, launch_dates, service_level=0.95):
        try:
            print("Initializing Enhanced Forecasting Model...")

            expected_columns = ['SKU', 'Channel', 'Date', 'Sales']
            missing_cols = [col for col in expected_columns if col not in historical_data.columns]
            if missing_cols:
                raise ValueError(f"Missing columns: {missing_cols}")

            # Clean SKU data more thoroughly
            historical_data['SKU'] = historical_data['SKU'].astype(str).str.strip()
            historical_data['Date'] = pd.to_datetime(historical_data['Date'], format='%m/%d/%Y', errors='coerce')
            historical_data = historical_data.dropna(subset=['Date'])
            historical_data['Channel'] = historical_data['Channel'].str.strip().str.title()
            historical_data = historical_data[historical_data['Channel'].isin(['Amazon', 'Shopify', 'Shopify Faire', 'Amazonfbm', 'Walmartfbm'])]
            historical_data['Sales'] = pd.to_numeric(historical_data['Sales'], errors='coerce').fillna(0)

            self.data = historical_data
            self.lead_times = lead_times
            self.launch_dates = launch_dates
            self.service_level = service_level
            self.z_score = norm.ppf(service_level)

            # Debug SKU matching
            print(f"\n🔍 SKU MATCHING DEBUG:")
            historical_skus = set(self.data['SKU'].unique())
            lead_time_skus = set(lead_times.keys())

            print(f"Historical data SKUs: {len(historical_skus)}")
            print(f"Lead times SKUs: {len(lead_time_skus)}")

            # Show sample SKUs from each source
            print(f"Sample historical SKUs: {list(historical_skus)[:5]}")
            print(f"Sample lead time SKUs: {list(lead_time_skus)[:5]}")

            # Check for matches
            matching_skus = historical_skus.intersection(lead_time_skus)
            print(f"Directly matching SKUs: {len(matching_skus)}")

            print(f"Data processed: {len(self.data)} records for {self.data['SKU'].nunique()} SKUs")

            self.velocity_categories = self._perform_abc_analysis()
            print(f"ABC Analysis complete: {len(self.velocity_categories)} SKUs categorized")

        except Exception as e:
            print(f"Error initializing model: {e}")
            raise

    def _perform_abc_analysis(self):
        try:
            print("Performing ABC velocity analysis...")

            latest_date = self.data['Date'].max()
            cutoff_date = latest_date - pd.DateOffset(months=3)
            recent_data = self.data[self.data['Date'] >= cutoff_date]

            if recent_data.empty:
                recent_data = self.data.copy()

            sku_sales = recent_data.groupby('SKU')['Sales'].sum().sort_values(ascending=False)

            categories = {}
            total_skus = len(sku_sales)

            for i, (sku, sales) in enumerate(sku_sales.items()):
                rank = i + 1

                if rank <= 20 or rank <= total_skus * 0.10:
                    category = 'A'
                    safety_months = 6.0
                    service_level = 0.98
                elif rank <= total_skus * 0.30:
                    category = 'B'
                    safety_months = 6.0
                    service_level = 0.95
                elif rank <= total_skus * 0.70:
                    category = 'C'
                    safety_months = 3.0
                    service_level = 0.90
                else:
                    category = 'D'
                    safety_months = 3.0
                    service_level = 0.85

                categories[sku] = {
                    'category': category,
                    'rank': rank,
                    'total_sales': sales,
                    'monthly_velocity': sales / 3,
                    'safety_stock_months': safety_months,
                    'service_level': service_level
                }

            return categories

        except Exception as e:
            print(f"Error in ABC analysis: {e}")
            return {}

    def smart_sku_lookup(self, sku, product_info, data_dict=None):
        """
        Enhanced SKU lookup that tries multiple matching strategies
        Can return product info or any other data from data_dict
        """
        sku = str(sku).strip()

        # Choose which dictionary to use for lookup
        lookup_dict = data_dict if data_dict is not None else product_info

        # Direct match first
        if sku in lookup_dict:
            return lookup_dict[sku]

        # Try variations of the SKU
        sku_variations = [
            sku.lstrip('0'),  # Remove leading zeros
            sku.zfill(12),    # Pad with zeros to 12 digits
            ''.join(c for c in sku if c.isalnum()),  # Remove special characters
            sku.replace(' ', '').replace('-', '')     # Remove spaces and dashes
        ]

        for variant in sku_variations:
            if variant and variant in lookup_dict:
                return lookup_dict[variant]

        # Try partial matching (both ways)
        for lookup_sku, value in lookup_dict.items():
            # Check if either SKU contains the other
            if len(sku) >= 8 and len(lookup_sku) >= 8:  # Only for reasonable length SKUs
                if sku in lookup_sku or lookup_sku in sku:
                    return value

        # Return appropriate default based on data type
        if data_dict is None:  # product_info lookup
            return 'Unknown'
        else:  # Could be launch_dates or other data
            return None

    def calculate_years_since_launch(self, sku, current_date=None):
        """
        Calculate the number of years since product launch (with decimal precision)
        """
        if current_date is None:
            current_date = pd.Timestamp.now()

        launch_date = self.smart_sku_lookup(sku, None, self.launch_dates)

        if launch_date is None or pd.isna(launch_date):
            return None

        try:
            # Calculate the difference in years with decimal precision
            years_diff = (current_date - launch_date).days / 365.25  # Account for leap years
            return round(years_diff, 1) if years_diff >= 0 else 0.0
        except:
            return None

    def prepare_data(self, channel, sku):
        try:
            sku = str(sku).strip()
            channel = str(channel).strip().title()

            sku_data = self.data[(self.data['SKU'] == sku) & (self.data['Channel'] == channel)].sort_values('Date')

            if sku_data.empty:
                date_range = pd.date_range(start='2024-01-01', end='2025-04-30', freq='ME')
                return pd.Series([0] * len(date_range), index=date_range)

            sku_data['Month'] = sku_data['Date'].dt.to_period('M').dt.to_timestamp('M')
            monthly_sales = sku_data.groupby('Month')['Sales'].sum().sort_index()

            full_range = pd.date_range(start=monthly_sales.index.min(), end=monthly_sales.index.max(), freq='ME')
            monthly_sales = monthly_sales.reindex(full_range, fill_value=0)
            monthly_sales.index.freq = 'ME'

            return monthly_sales

        except Exception:
            date_range = pd.date_range(start='2024-01-01', end='2025-04-30', freq='ME')
            return pd.Series([0] * len(date_range), index=date_range)

    def generate_forecast(self, series, horizon=8):
        try:
            growth_rate = self._calculate_growth_rate(series)

            if series.empty or series.sum() == 0:
                last_date = series.index[-1] if not series.empty else pd.to_datetime('today') + pd.offsets.MonthEnd(0)
                forecast_dates = pd.date_range(start=last_date + pd.offsets.MonthEnd(1), periods=horizon, freq='ME')
                forecast = pd.Series([0] * horizon, index=forecast_dates)
                return forecast, 'Zero Forecast', 0, [], 0

            mean_sales = series.mean()
            std_sales = series.std()
            cv = std_sales / mean_sales if mean_sales > 0 else float('inf')

            if cv > 3 or len(series) < 3:
                avg_forecast = max(mean_sales, 0)
                forecast_values = [max(0, round(avg_forecast + growth_rate * (i + 1))) for i in range(horizon)]
                last_date = series.index[-1]
                forecast_dates = pd.date_range(start=last_date + pd.offsets.MonthEnd(1), periods=horizon, freq='ME')
                forecast = pd.Series(forecast_values, index=forecast_dates)
                return forecast, 'Average + Growth', 0, [], growth_rate

            try:
                if len(series) >= 12:
                    model = ExponentialSmoothing(series, seasonal='add', seasonal_periods=6, trend='add').fit()
                    method = 'Holt-Winters Seasonal'
                elif len(series) >= 6:
                    model = ExponentialSmoothing(series, trend='add').fit()
                    method = 'Holt-Winters Trend'
                else:
                    model = ExponentialSmoothing(series).fit()
                    method = 'Simple Exponential Smoothing'

                forecast = model.forecast(horizon)
                forecast = forecast.clip(lower=0, upper=series.max() * 5)

                last_date = series.index[-1]
                forecast_dates = pd.date_range(start=last_date + pd.offsets.MonthEnd(1), periods=horizon, freq='ME')
                forecast.index = forecast_dates

                return forecast.round().astype(int), method, 6, [], growth_rate

            except Exception:
                avg_forecast = max(mean_sales, 0)
                forecast_values = [max(0, round(avg_forecast + growth_rate * (i + 1))) for i in range(horizon)]
                last_date = series.index[-1]
                forecast_dates = pd.date_range(start=last_date + pd.offsets.MonthEnd(1), periods=horizon, freq='ME')
                forecast = pd.Series(forecast_values, index=forecast_dates)
                return forecast, 'Fallback Average', 0, [], growth_rate

        except Exception:
            last_date = pd.to_datetime('2025-04-30')
            forecast_dates = pd.date_range(start=last_date + pd.offsets.MonthEnd(1), periods=horizon, freq='ME')
            forecast = pd.Series([0] * horizon, index=forecast_dates)
            return forecast, 'Error Fallback', 0, [], 0

    def _calculate_growth_rate(self, series):
        try:
            if len(series) < 2:
                return 0
            x = np.arange(len(series)).reshape(-1, 1)
            y = series.values.reshape(-1, 1)
            model = LinearRegression()
            model.fit(x, y)
            growth_rate = round(model.coef_[0][0], 2)
            # Handle NaN/Inf values
            if pd.isna(growth_rate) or growth_rate == float('inf') or growth_rate == float('-inf'):
                return 0
            return growth_rate
        except:
            return 0

    def calculate_enhanced_safety_stock(self, series, lead_time, sku):
        try:
            sku_info = self.velocity_categories.get(sku, {})
            category = sku_info.get('category', 'D')
            target_service_level = sku_info.get('service_level', 0.85)
            safety_months = sku_info.get('safety_stock_months', 3.0)

            z_score = norm.ppf(target_service_level)

            if len(series) < 2 or series.sum() == 0:
                return max(round(safety_months * 10), 5), category, safety_months

            demand_std = series.std()
            mean_demand = series.mean()

            statistical_ss = z_score * demand_std * math.sqrt(lead_time)
            velocity_min_ss = mean_demand * safety_months

            final_ss = max(statistical_ss, velocity_min_ss)
            min_ss = max(5, mean_demand * 0.25)
            max_ss = mean_demand * lead_time * 2
            final_ss = max(min_ss, min(final_ss, max_ss))

            return max(round(final_ss), 5), category, safety_months

        except Exception:
            return 10, 'D', 1.0

    def calculate_enhanced_reorder_point(self, series, lead_time, safety_stock):
        try:
            if series.empty or series.sum() == 0:
                return safety_stock

            avg_demand = series.mean() * lead_time
            growth_rate = self._calculate_growth_rate(series)
            trend_adjustment = growth_rate * lead_time

            reorder_point = avg_demand + trend_adjustment + safety_stock
            return max(round(reorder_point), safety_stock)

        except:
            return safety_stock

    def calculate_enhanced_po_quantity(self, series, current_inventory, reorder_point, sku, safety_stock_months):
        try:
            if series.empty or series.sum() == 0:
                if current_inventory < reorder_point:
                    return max(20, reorder_point - current_inventory), "NEW PRODUCT - Order Required"
                else:
                    return 0, "NEW PRODUCT - Sufficient Stock"

            monthly_demand = series.mean()
            if monthly_demand <= 0:
                return 0, "No demand"

            sku_info = self.velocity_categories.get(sku, {})
            velocity_category = sku_info.get('category', 'D')

            if velocity_category == 'A':
                order_months = 4.0
            elif velocity_category == 'B':
                order_months = 3.0
            elif velocity_category == 'C':
                order_months = 2.5
            else:
                order_months = 2.0

            if current_inventory <= reorder_point:
                shortage = reorder_point - current_inventory
                target_stock = monthly_demand * (safety_stock_months + order_months)
                po_quantity = max(shortage, target_stock - current_inventory)
                urgency = "HIGH - Below Reorder Point"
            elif current_inventory <= reorder_point * 1.3:
                po_quantity = monthly_demand * order_months
                urgency = "MEDIUM - Approaching Reorder Point"
            else:
                po_quantity = 0
                urgency = "LOW - Sufficient Stock"

            return round(po_quantity), urgency

        except Exception:
            return 0, "Error"

    def calculate_future_orders(self, forecast, current_inventory, reorder_point, lead_time, safety_stock_months, sku):
        try:
            orders = []
            running_inventory = current_inventory

            sku_info = self.velocity_categories.get(sku, {})
            velocity_category = sku_info.get('category', 'D')

            if velocity_category == 'A':
                order_months = 4.0
            elif velocity_category == 'B':
                order_months = 3.0
            elif velocity_category == 'C':
                order_months = 2.5
            else:
                order_months = 2.0

            forecast_values = [f for f in forecast.iloc[:6] if f > 0]
            if not forecast_values:
                return [], [], []

            avg_monthly_demand = sum(forecast_values) / len(forecast_values)
            order_quantity = round(avg_monthly_demand * order_months)

            for i in range(8):
                month_demand = forecast.iloc[i] if i < len(forecast) else 0

                if i + lead_time < 8:
                    future_inventory = running_inventory

                    for j in range(i, min(i + lead_time, 8)):
                        future_demand = forecast.iloc[j] if j < len(forecast) else 0
                        future_inventory -= future_demand

                    if future_inventory <= reorder_point and order_quantity > 0:
                        order_date = forecast.index[i] if i < len(forecast) else None
                        arrival_date = forecast.index[min(i + lead_time, len(forecast)-1)] if i + lead_time < len(forecast) else None

                        orders.append({
                            'order_date': order_date,
                            'arrival_date': arrival_date,
                            'quantity': order_quantity
                        })

                        if i + lead_time < 8:
                            running_inventory += order_quantity

                running_inventory -= month_demand
                running_inventory = max(0, running_inventory)

            order_dates = []
            order_qtys = []
            arrival_dates = []

            for order in orders[:3]:
                order_dates.append(order['order_date'].strftime('%Y-%m-%d') if order['order_date'] else '')
                order_qtys.append(order['quantity'])
                arrival_dates.append(order['arrival_date'].strftime('%Y-%m-%d') if order['arrival_date'] else '')

            while len(order_dates) < 3:
                order_dates.append('')
                order_qtys.append(0)
                arrival_dates.append('')

            return order_dates, order_qtys, arrival_dates

        except Exception:
            return ['', '', ''], [0, 0, 0], ['', '', '']

    def calculate_months_of_inventory(self, current_inventory, forecast):
        try:
            if current_inventory <= 0:
                return 0.0

            forecast_values = [f for f in forecast.iloc[:6] if f > 0]

            if not forecast_values or sum(forecast_values) == 0:
                return 999.0

            avg_monthly_demand = sum(forecast_values) / len(forecast_values)

            if avg_monthly_demand <= 0:
                return 999.0

            months_available = current_inventory / avg_monthly_demand
            return round(months_available, 1)

        except Exception:
            return 0.0

    def create_enhanced_forecast(self, channel, inventory, product_info, product_category, product_status):
        try:
            print(f"Creating enhanced forecast for {channel}...")

            all_skus = set()
            all_skus.update(self.lead_times.keys())
            all_skus.update(inventory.keys())
            all_skus.update(product_info.keys())
            all_skus.update(self.data['SKU'].unique())
            all_skus.update(product_category.keys())
            all_skus.update(product_status.keys())

            print(f"Processing {len(all_skus)} SKUs for {channel}")

            # Generate historical month labels (last 3 months)
            current_date = pd.Timestamp.now()
            historical_months = []
            for i in range(3, 0, -1):  # 3, 2, 1 months ago
                past_date = current_date - pd.DateOffset(months=i)
                historical_months.append(past_date.strftime('%b_%Y'))

            # Generate forecast month labels
            forecast_months = []
            for i in range(8):
                future_date = current_date + pd.DateOffset(months=i+1)
                forecast_months.append(future_date.strftime('%b_%Y'))  # e.g., 'Jun_2025'

            print(f"Historical periods: {', '.join(historical_months)}")
            print(f"Forecast periods: {', '.join(forecast_months)}")

            forecast_data = []
            products_found = 0

            for i, sku in enumerate(all_skus):
                try:
                    if i % 20 == 0:
                        print(f"   Processing SKU {i+1}/{len(all_skus)}: {sku}")

                    series = self.prepare_data(channel, sku)
                    forecast, method, seasonal_periods, seasonal_factors, growth_rate = self.generate_forecast(series)

                    # Use enhanced SKU lookup for product names and launch dates
                    product_name = self.smart_sku_lookup(sku, product_info)

                    category_name = self.smart_sku_lookup(sku, product_category)
                    status_name = self.smart_sku_lookup(sku, product_status)

                    launch_date = self.smart_sku_lookup(sku, None, self.launch_dates)
                    years_since_launch = self.calculate_years_since_launch(sku)

                    # Debug: Show some matching attempts
                    if i < 5 or (i % 50 == 0):
                        print(f"   DEBUG SKU {sku}: Product = '{product_name}'")

                    if product_name != 'Unknown':
                        products_found += 1
                    else:
                        # Skip forecasts for SKUs without valid product mappings
                        if i % 100 == 0:  # Only log occasionally to avoid spam
                            print(f"   Skipping SKU {sku} - no product mapping found")
                        continue

                    # Get last 3 months sales (skip the most recent month to avoid incomplete data)
                    last_3_sales = series.iloc[-4:-1].tolist() if len(series) >= 4 else series.tail(3).tolist() if len(series) >= 3 else [0, 0, 0]
                    while len(last_3_sales) < 3:
                        last_3_sales.insert(0, 0)
                    last_3_sales = last_3_sales[-3:]  # Ensure exactly 3 values

                    # Ensure we have valid numeric values
                    last_3_sales = [float(x) if not pd.isna(x) else 0.0 for x in last_3_sales]

                    last_3_months_avg = round(series.iloc[-4:-1].mean(), 2) if len(series) >= 4 else round(series[-3:].mean(), 2) if len(series) >= 3 else round(series.mean(), 2) if not series.empty else 0
                    # Handle NaN values
                    if pd.isna(last_3_months_avg):
                        last_3_months_avg = 0.0

                    total_sales = round(series.sum(), 2)
                    if pd.isna(total_sales):
                        total_sales = 0.0
                    lead_time = self.lead_times.get(sku, 2)

                    safety_stock, velocity_category, safety_stock_months = self.calculate_enhanced_safety_stock(series, lead_time, sku)
                    reorder_point = self.calculate_enhanced_reorder_point(series, lead_time, safety_stock)

                    current_inventory = inventory.get(sku, 0)
                    po_quantity, urgency = self.calculate_enhanced_po_quantity(series, current_inventory, reorder_point, sku, safety_stock_months)

                    order_dates, order_qtys, arrival_dates = self.calculate_future_orders(forecast, current_inventory, reorder_point, lead_time, safety_stock_months, sku)
                    months_of_inventory = self.calculate_months_of_inventory(current_inventory, forecast)

                    sku_info = self.velocity_categories.get(sku, {})

                    if current_inventory <= 0:
                        stock_status = "OUT OF STOCK"
                    elif current_inventory <= reorder_point:
                        stock_status = "REORDER NOW"
                    elif last_3_months_avg > 0 and current_inventory / last_3_months_avg < 1:
                        stock_status = "LOW STOCK"
                    elif last_3_months_avg > 0 and current_inventory / last_3_months_avg > 6:
                        stock_status = "OVERSTOCK"
                    else:
                        stock_status = "NORMAL"

                    # Create dynamic forecast row with required order
                    row_data = {
                        'SKU': str(sku),
                        'Product_Name': product_name,
                        'Category': category_name,
                        'Status': status_name,
                        'Launch_Date': launch_date.strftime('%Y-%m-%d') if launch_date and not pd.isna(launch_date) else '',
                        'Forecast_Method': method,
                        'Years_Since_Launch': years_since_launch if years_since_launch is not None else '',
                        'Current_Inventory': current_inventory,
                        'Stock_Status': stock_status,
                        'PO_Urgency': urgency,
                        'Recommended_PO_Qty': po_quantity,
                        'Next_Order_Date': order_dates[0],
                        'Next_Order_Qty': order_qtys[0],
                        'Next_Arrival_Date': arrival_dates[0],
                        'Months_of_Inventory': months_of_inventory,
                        'Velocity_Category': velocity_category,
                        'Safety_Stock_Months': safety_stock_months,
                        'Reorder_Point': reorder_point,
                        'Safety_Stock': safety_stock,
                    }
                    ## AMAZON TAB
                    # Add forecast columns with month/year labels
                    for idx, month_label in enumerate(forecast_months):
                        row_data[f'Forecast_{month_label}'] = int(forecast.iloc[idx]) if idx < len(forecast) else 0

                    # Add trailing metrics and info (except Channel and Historical for now)
                    row_data.update({
                        'Last_3_Months_Avg': last_3_months_avg,
                        'Total_Sales': total_sales,
                        'Growth_Rate': growth_rate if not pd.isna(growth_rate) else 0.0,
                        'Order_2_Date': order_dates[1],
                        'Order_2_Qty': order_qtys[1],
                        'Order_2_Arrival': arrival_dates[1],
                        'Order_3_Date': order_dates[2],
                        'Order_3_Qty': order_qtys[2],
                        'Order_3_Arrival': arrival_dates[2],
                        'Lead_Time': lead_time,
                        'Service_Level': f"{sku_info.get('service_level', 0.85)*100:.0f}%",
                        'Monthly_Velocity': round(sku_info.get('monthly_velocity', 0), 1) if not pd.isna(sku_info.get('monthly_velocity', 0)) else 0.0,
                        'Velocity_Rank': sku_info.get('rank', 999),
                    })

                    # Add historical months data right before 'Channel'
                    for idx, month_label in enumerate(historical_months):
                        if idx < len(last_3_sales):
                            value = int(last_3_sales[idx]) if not pd.isna(last_3_sales[idx]) else 0
                        else:
                            value = 0
                        row_data[month_label] = value

                    # Now finally add 'Channel' at the end
                    row_data['Channel'] = channel

                    # Append row to forecast list
                    forecast_data.append(row_data)


                except Exception as e:
                    print(f"   Error processing SKU {sku}: {e}")
                    if i < 10:  # Show detailed errors for first 10 SKUs
                        import traceback
                        traceback.print_exc()
                    continue

            df = pd.DataFrame(forecast_data)

            # Additional filtering to ensure we only have mapped products
            if not df.empty:
                initial_count = len(df)
                df = df[df['Product_Name'] != 'Unknown']
                final_count = len(df)
                filtered_out = initial_count - final_count

                if filtered_out > 0:
                    print(f"   Filtered out {filtered_out} SKUs without product mappings")

            print(f"Generated {len(df)} forecasts for {channel} (only mapped products)")
            print(f"✅ Successfully mapped {products_found} product names")

            return df

        except Exception:
            return pd.DataFrame()

    def combine_channel_forecasts(self, amazon_forecast, shopify_forecast, shopify_faire_forecast, amazon_fbm_forecast, walmart_fbm_forecast):
        try:
            print("Combining channel forecasts...")

            # Concatenate all dataframes
            all_data = pd.concat([amazon_forecast, shopify_forecast, shopify_faire_forecast, amazon_fbm_forecast, walmart_fbm_forecast], ignore_index=True)

            if all_data.empty:
                return pd.DataFrame()
            
            # Get forecast month columns dynamically
            forecast_columns = [col for col in all_data.columns if col.startswith('Forecast_') and col != 'Forecast_Method']

            # Get historical month columns dynamically
            historical_columns = []
            month_names = ['Jan_', 'Feb_', 'Mar_', 'Apr_', 'May_', 'Jun_', 'Jul_', 'Aug_', 'Sep_', 'Oct_', 'Nov_', 'Dec_']
            for col in all_data.columns:
                if any(month in col for month in month_names) and 'Forecast_' not in col and '_' in col:
                    try:
                        parts = col.split('_')
                        if len(parts) == 2 and parts[0][:3] in [m[:3] for m in month_names]:
                            historical_columns.append(col)
                    except:
                        pass

            print(f"Found historical columns: {historical_columns}")
            print(f"Found forecast columns: {forecast_columns}")

            combined_data = []
            unique_skus = all_data['SKU'].unique()

            for sku in unique_skus:
                sku_data = all_data[all_data['SKU'] == sku]

                if sku_data.empty:
                    continue

                # Get data from each channel for this SKU
                amazon_data = sku_data[sku_data['Channel'] == 'Amazon']
                shopify_data = sku_data[sku_data['Channel'] == 'Shopify']
                shopify_faire_data = sku_data[sku_data['Channel'] == 'Shopify Faire']
                amazon_fbm_data = sku_data[sku_data['Channel'] == 'Amazonfbm']
                walmart_fbm_data = sku_data[sku_data['Channel'] == 'Walmartfbm']

                # Use the first available record as base (preference: Amazon -> Shopify -> others)
                if not amazon_data.empty:
                    base_record = amazon_data.iloc[0].copy()
                elif not shopify_data.empty:
                    base_record = shopify_data.iloc[0].copy()
                else:
                    base_record = sku_data.iloc[0].copy()

                ### ALL FORECASTS TAB - maintaining original column sequence
                combined_record = {
                    'SKU': str(sku),
                    'Product_Name': base_record.get('Product_Name', 'Unknown'),
                    'Category': base_record.get('Category', 'Unknown'),
                    'Status': base_record.get('Status', 'Unknown'),
                    'Launch_Date': base_record.get('Launch_Date', ''),
                    'Years_Since_Launch': base_record.get('Years_Since_Launch', ''),
                    'Velocity_Category': base_record.get('Velocity_Category', 'D'),
                    'Velocity_Rank': base_record.get('Velocity_Rank', 999),
                    'Service_Level': base_record.get('Service_Level', '85%'),
                    'Safety_Stock_Months': base_record.get('Safety_Stock_Months', 1.0),
                    'Months_of_Inventory': base_record.get('Months_of_Inventory', 0.0),
                    'Safety_Stock': base_record.get('Safety_Stock', 0),
                    'Reorder_Point': base_record.get('Reorder_Point', 0),
                    'Current_Inventory': base_record.get('Current_Inventory', 0),
                }

                # Calculate Last_3_Months_Avg from summed historical data
                if len(historical_columns) >= 3:
                    last_3_sum = sum(int(sku_data[col].sum()) for col in historical_columns[-3:] if col in sku_data.columns)
                    combined_record['Last_3_Months_Avg'] = round(last_3_sum / 3, 2)
                else:
                    combined_record['Last_3_Months_Avg'] = 0.0

                # Add Growth_Rate
                combined_record['Growth_Rate'] = round(sku_data['Growth_Rate'].mean(), 2) if 'Growth_Rate' in sku_data.columns and not pd.isna(sku_data['Growth_Rate'].mean()) else 0.0

                # Add remaining fields
                combined_record.update({
                    'Stock_Status': base_record.get('Stock_Status', 'UNKNOWN'),
                    'PO_Urgency': base_record.get('PO_Urgency', 'LOW'),
                    'Recommended_PO_Qty': base_record.get('Recommended_PO_Qty', 0),
                    'Next_Order_Date': base_record.get('Next_Order_Date', ''),
                    'Next_Order_Qty': base_record.get('Next_Order_Qty', 0),
                    'Next_Arrival_Date': base_record.get('Next_Arrival_Date', ''),
                })

                # Add combined forecast columns (summed across channels)
                for col in forecast_columns:
                    combined_record[col] = int(sku_data[col].sum()) if col in sku_data.columns else 0

                # Add channel breakdown for first 3 forecast months
                if len(forecast_columns) >= 3:
                    for i in range(3):
                        month_col = forecast_columns[i]
                        month_label = month_col.replace('Forecast_', '')
                        combined_record[f'Amazon_{month_label}'] = int(amazon_data[month_col].iloc[0]) if not amazon_data.empty and month_col in amazon_data.columns else 0
                        combined_record[f'Shopify_{month_label}'] = int(shopify_data[month_col].iloc[0]) if not shopify_data.empty and month_col in shopify_data.columns else 0
                        combined_record[f'Shopify_Faire_{month_label}'] = int(shopify_faire_data[month_col].iloc[0]) if not shopify_faire_data.empty and month_col in shopify_faire_data.columns else 0
                        combined_record[f'Amazon_FBM_{month_label}'] = int(amazon_fbm_data[month_col].iloc[0]) if not amazon_fbm_data.empty and month_col in amazon_fbm_data.columns else 0
                        combined_record[f'Walmart_FBM_{month_label}'] = int(walmart_fbm_data[month_col].iloc[0]) if not walmart_fbm_data.empty and month_col in walmart_fbm_data.columns else 0

                # Add remaining metrics
                combined_record.update({
                    'Total_Sales': round(sku_data['Total_Sales'].sum(), 2) if 'Total_Sales' in sku_data.columns and not pd.isna(sku_data['Total_Sales'].sum()) else 0.0,
                    'Order_2_Date': base_record.get('Order_2_Date', ''),
                    'Order_2_Qty': base_record.get('Order_2_Qty', 0),
                    'Order_2_Arrival': base_record.get('Order_2_Arrival', ''),
                    'Order_3_Date': base_record.get('Order_3_Date', ''),
                    'Order_3_Qty': base_record.get('Order_3_Qty', 0),
                    'Order_3_Arrival': base_record.get('Order_3_Arrival', ''),
                    'Lead_Time': base_record.get('Lead_Time', 2),
                    'Monthly_Velocity': base_record.get('Monthly_Velocity', 0) if not pd.isna(base_record.get('Monthly_Velocity', 0)) else 0.0,
                    'Channel': 'Combined'
                })

                # Add historical months just before Channel
                for col in historical_columns:
                    combined_record[col] = int(sku_data[col].sum()) if col in sku_data.columns else 0

                # Forecast_Method last
                combined_record['Forecast_Method'] = "Combined Channels"

                combined_data.append(combined_record)

            combined_df = pd.DataFrame(combined_data)
            print(f"Combined forecasts: {len(combined_df)} unique SKUs")

            return combined_df

        except Exception as e:
            print(f"Error in combine_channel_forecasts: {str(e)}")
            return pd.concat([amazon_forecast, shopify_forecast, shopify_faire_forecast, amazon_fbm_forecast, walmart_fbm_forecast], ignore_index=True)

    def generate_actionable_insights(self, combined_forecast):
        """Generate comprehensive actionable insights for inventory planning."""
        try:
            print("Generating actionable insights...")

            insights = {
                'immediate_actions': [],
                'weekly_actions': [],
                'monthly_actions': [],
                'risk_analysis': [],
                'opportunities': [],
                'cost_optimization': []
            }

            current_date = pd.Timestamp.now()

            for _, row in combined_forecast.iterrows():
                row_dict = row.to_dict()

                sku = row_dict.get('SKU', '')
                product_name = row_dict.get('Product_Name', '')
                current_inventory = row_dict.get('Current_Inventory', 0)
                months_of_inventory = row_dict.get('Months_of_Inventory', 0.0)
                po_urgency = row_dict.get('PO_Urgency', 'LOW')
                velocity_category = row_dict.get('Velocity_Category', 'D')
                reorder_point = row_dict.get('Reorder_Point', 0)
                recommended_po = row_dict.get('Recommended_PO_Qty', 0)
                lead_time = row_dict.get('Lead_Time', 2)
                growth_rate = row_dict.get('Growth_Rate', 0.0)

                # Sanitize invalid or infinite values
                if pd.isna(months_of_inventory) or months_of_inventory == float('inf'):
                    months_of_inventory = 999.0
                elif months_of_inventory == float('-inf'):
                    months_of_inventory = 0.0
                if pd.isna(growth_rate):
                    growth_rate = 0.0

                # Get forecast columns dynamically
                forecast_cols = [col for col in row_dict if col.startswith('Forecast_')]
                next_3_months_demand = 0
                for col in forecast_cols[:3]:
                    try:
                        val = float(row.get(col, 0))
                        if pd.notna(val):
                            next_3_months_demand += val
                    except:
                        continue


                # === IMMEDIATE ACTIONS ===
                if current_inventory <= 0:
                    lost_sales = next_3_months_demand * 50
                    insights['immediate_actions'].append({
                        'Priority': 'CRITICAL',
                        'SKU': sku,
                        'Product': product_name,
                        'Action': 'EXPEDITE ORDER IMMEDIATELY',
                        'Reason': 'OUT OF STOCK',
                        'Quantity': max(recommended_po, next_3_months_demand),
                        'Impact': f'Lost sales: ~${lost_sales:.0f}/month',
                        'Contact': 'Call supplier TODAY for expedited shipping'
                    })
                elif current_inventory <= reorder_point * 0.5:
                    insights['immediate_actions'].append({
                        'Priority': 'HIGH',
                        'SKU': sku,
                        'Product': product_name,
                        'Action': 'Place PO Today',
                        'Reason': f'Critically low: {months_of_inventory:.1f} months left',
                        'Quantity': recommended_po,
                        'Impact': f'Risk of stockout in {lead_time} months',
                        'Contact': 'Email PO to supplier by EOD'
                    })

                # === WEEKLY ACTIONS ===
                elif current_inventory <= reorder_point:
                    insights['weekly_actions'].append({
                        'Priority': 'MEDIUM',
                        'SKU': sku,
                        'Product': product_name,
                        'Action': 'Place PO This Week',
                        'Reason': 'At reorder point',
                        'Quantity': recommended_po,
                        'By_Date': (current_date + pd.Timedelta(days=7)).strftime('%Y-%m-%d'),
                        'Notes': f'Lead time: {lead_time} months'
                    })

                # === MONTHLY ACTIONS ===
                next_order_date = row_dict.get('Next_Order_Date')
                next_order_qty = row_dict.get('Next_Order_Qty', 0)

                if next_order_date:
                    try:
                        parsed_date = pd.to_datetime(next_order_date)
                        days_until_order = (parsed_date - current_date).days
                        if 7 < days_until_order <= 30:
                            budget_impact = next_order_qty * 30
                            insights['monthly_actions'].append({
                                'SKU': sku,
                                'Product': product_name,
                                'Action': 'Schedule PO',
                                'Order_Date': next_order_date,
                                'Quantity': next_order_qty,
                                'Preparation': 'Confirm supplier capacity',
                                'Budget_Impact': f'~${budget_impact:.0f}'
                            })
                    except Exception:
                        pass

                # === RISK ANALYSIS ===
                if velocity_category == 'A' and months_of_inventory < 2:
                    potential_loss = next_3_months_demand * 50
                    insights['risk_analysis'].append({
                        'Risk_Level': 'HIGH',
                        'SKU': sku,
                        'Product': product_name,
                        'Issue': 'Top seller with low inventory',
                        'Potential_Loss': f'${potential_loss:.0f}',
                        'Mitigation': 'Consider air freight or split shipments'
                    })
                elif months_of_inventory > 12:
                    tied_capital = current_inventory * 30
                    insights['risk_analysis'].append({
                        'Risk_Level': 'MEDIUM',
                        'SKU': sku,
                        'Product': product_name,
                        'Issue': f'Excess inventory: {months_of_inventory:.0f} months',
                        'Tied_Capital': f'${tied_capital:.0f}',
                        'Mitigation': 'Pause orders, consider promotions'
                    })

                # === OPPORTUNITIES ===
                if growth_rate > 10 and months_of_inventory < 3:
                    insights['opportunities'].append({
                        'Type': 'GROWTH',
                        'SKU': sku,
                        'Product': product_name,
                        'Trend': f'+{growth_rate:.0f}% growth',
                        'Action': 'Increase safety stock',
                        'Potential': 'Capture more market share'
                    })

                # === COST OPTIMIZATION ===
                if velocity_category in ['C', 'D'] and recommended_po > 0:
                    insights['cost_optimization'].append({
                        'SKU': sku,
                        'Product': product_name,
                        'Current_Order': recommended_po,
                        'Suggestion': 'Combine with other orders',
                        'Savings': 'Reduce shipping costs by 15-20%',
                        'Action': 'Consolidate low-velocity orders monthly'
                    })

            print(f"📌 immediate_actions count: {len(insights['immediate_actions'])}")
            print(f"📌 weekly_actions count: {len(insights['weekly_actions'])}")
            print(f"📌 monthly_actions count: {len(insights['monthly_actions'])}")
            print(f"📌 risk_analysis count: {len(insights['risk_analysis'])}")
            print(f"📌 opportunities count: {len(insights['opportunities'])}")
            print(f"📌 cost_optimization count: {len(insights['cost_optimization'])}")

            return insights

        except Exception as e:
            print(f"❌ Error generating insights: {e}")
            import traceback
            traceback.print_exc()
            return {}

    def create_executive_summary(self, combined_forecast, insights):
        """Create executive summary with key metrics and actions."""
        try:
            summary = {
                'date': pd.Timestamp.now().strftime('%Y-%m-%d'),
                'total_skus': len(combined_forecast),
                'immediate_actions_required': len(insights.get('immediate_actions', [])),
                'weekly_actions_required': len(insights.get('weekly_actions', [])),
                'at_risk_skus': 0,
                'overstock_skus': 0,
                'total_inventory_value': 0,
                'total_po_value_needed': 0,
                'cash_flow_30_days': 0,
                'cash_flow_60_days': 0,
                'cash_flow_90_days': 0
            }

            # Calculate key metrics
            for _, row in combined_forecast.iterrows():
                current_inv = row['Current_Inventory']
                months_inv = row['Months_of_Inventory']

                # Inventory value (assuming $30 cost)
                summary['total_inventory_value'] += current_inv * 30

                # At risk SKUs
                if months_inv < 2:
                    summary['at_risk_skus'] += 1
                elif months_inv > 6:
                    summary['overstock_skus'] += 1

                # PO values needed
                if row['Recommended_PO_Qty'] > 0:
                    summary['total_po_value_needed'] += row['Recommended_PO_Qty'] * 30

                # Cash flow projections
                if row['Next_Order_Date']:
                    order_date = pd.to_datetime(row['Next_Order_Date'])
                    days_until = (order_date - pd.Timestamp.now()).days
                    order_value = row['Next_Order_Qty'] * 30

                    if days_until <= 30:
                        summary['cash_flow_30_days'] += order_value
                    elif days_until <= 60:
                        summary['cash_flow_60_days'] += order_value
                    elif days_until <= 90:
                        summary['cash_flow_90_days'] += order_value

            return summary

        except Exception as e:
            print(f"Error creating executive summary: {e}")
            return {}

    def create_action_priority_matrix(self, combined_forecast):
        """Create action priority matrix based on velocity and urgency."""
        try:
            priority_matrix = []

            for _, row in combined_forecast.iterrows():
                velocity = row['Velocity_Category']
                months_inv = row['Months_of_Inventory']
                po_urgency = row['PO_Urgency']

                # Calculate priority score
                velocity_score = {'A': 4, 'B': 3, 'C': 2, 'D': 1}.get(velocity, 1)

                if months_inv <= 1:
                    urgency_score = 4
                elif months_inv <= 2:
                    urgency_score = 3
                elif months_inv <= 3:
                    urgency_score = 2
                else:
                    urgency_score = 1

                priority_score = velocity_score * urgency_score

                if priority_score >= 12:
                    action_priority = 'IMMEDIATE'
                    action_timeline = 'Today'
                elif priority_score >= 8:
                    action_priority = 'HIGH'
                    action_timeline = 'This Week'
                elif priority_score >= 4:
                    action_priority = 'MEDIUM'
                    action_timeline = 'This Month'
                else:
                    action_priority = 'LOW'
                    action_timeline = 'Next Month'

                priority_matrix.append({
                    'SKU': row['SKU'],
                    'Product_Name': row['Product_Name'],
                    'Velocity_Category': velocity,
                    'Months_of_Inventory': months_inv if not pd.isna(months_inv) else 0.0,
                    'Priority_Score': priority_score,
                    'Action_Priority': action_priority,
                    'Action_Timeline': action_timeline,
                    'Recommended_Action': self._get_specific_action(row),
                    'Order_Quantity': row['Recommended_PO_Qty'] if not pd.isna(row['Recommended_PO_Qty']) else 0,
                    'Current_Inventory': row['Current_Inventory'] if not pd.isna(row['Current_Inventory']) else 0,
                    'Next_Month_Forecast': row[[col for col in row.index if col.startswith('Forecast_')][0]] if any(col.startswith('Forecast_') for col in row.index) and not pd.isna(row[[col for col in row.index if col.startswith('Forecast_')][0]]) else 0
                })

            return pd.DataFrame(priority_matrix)

        except Exception as e:
            print(f"Error creating priority matrix: {e}")
            return pd.DataFrame()

    def _get_specific_action(self, row):
        """Get specific action recommendation based on SKU status."""
        current_inv = row.get('Current_Inventory', 0)
        reorder_point = row.get('Reorder_Point', 0)
        months_inv = row.get('Months_of_Inventory', 0)

        # Handle NaN values
        if pd.isna(current_inv):
            current_inv = 0
        if pd.isna(reorder_point):
            reorder_point = 0
        if pd.isna(months_inv):
            months_inv = 0

        if current_inv <= 0:
            return 'EXPEDITE: Air freight required'
        elif current_inv <= reorder_point * 0.5:
            return 'URGENT: Place PO today, follow up with supplier'
        elif current_inv <= reorder_point:
            return 'REORDER: Standard PO this week'
        elif months_inv > 12:
            return 'PAUSE: No orders, plan clearance'
        elif months_inv > 6:
            return 'MONITOR: Reduce order quantities'
        else:
            return 'SCHEDULE: Follow standard ordering'
         
    def create_enhanced_forecast_shopify_special(self, channel, inventory, product_info, product_category, product_status):
        try:
            print(f"Creating enhanced forecast for {channel}...")

            all_skus = set()
            all_skus.update(self.lead_times.keys())
            all_skus.update(inventory.keys())
            all_skus.update(product_info.keys())
            all_skus.update(self.data['SKU'].unique())
            all_skus.update(product_category.keys())
            all_skus.update(product_status.keys())

            print(f"Processing {len(all_skus)} SKUs for {channel}")

            # Generate historical month labels (last 3 months)
            current_date = pd.Timestamp.now()
            historical_months = []
            for i in range(3, 0, -1):  # 3, 2, 1 months ago
                past_date = current_date - pd.DateOffset(months=i)
                historical_months.append(past_date.strftime('%b_%Y'))

            # Generate forecast month labels
            forecast_months = []
            for i in range(8):
                future_date = current_date + pd.DateOffset(months=i+1)
                forecast_months.append(future_date.strftime('%b_%Y'))  # e.g., 'Jun_2025'

            print(f"Historical periods: {', '.join(historical_months)}")
            print(f"Forecast periods: {', '.join(forecast_months)}")

            forecast_data = []
            products_found = 0

            for i, sku in enumerate(all_skus):
                try:
                    if i % 20 == 0:
                        print(f"   Processing SKU {i+1}/{len(all_skus)}: {sku}")

                    series = self.prepare_data(channel, sku)
                    forecast, method, seasonal_periods, seasonal_factors, growth_rate = self.generate_forecast(series)

                    # Use enhanced SKU lookup for product names and launch dates
                    product_name = self.smart_sku_lookup(sku, product_info)
                    
                    category_name = self.smart_sku_lookup(sku, product_category)
                    status_name = self.smart_sku_lookup(sku, product_status)

                    launch_date = self.smart_sku_lookup(sku, None, self.launch_dates)
                    years_since_launch = self.calculate_years_since_launch(sku)

                    # Debug: Show some matching attempts
                    if i < 5 or (i % 50 == 0):
                        print(f"   DEBUG SKU {sku}: Product = '{product_name}'")

                    if product_name != 'Unknown':
                        products_found += 1
                    else:
                        # Skip forecasts for SKUs without valid product mappings
                        if i % 100 == 0:  # Only log occasionally to avoid spam
                            print(f"   Skipping SKU {sku} - no product mapping found")
                        continue

                    # Get last 3 months sales (skip the most recent month to avoid incomplete data)
                    last_3_sales = series.iloc[-4:-1].tolist() if len(series) >= 4 else series.tail(3).tolist() if len(series) >= 3 else [0, 0, 0]
                    while len(last_3_sales) < 3:
                        last_3_sales.insert(0, 0)
                    last_3_sales = last_3_sales[-3:]  # Ensure exactly 3 values

                    # Ensure we have valid numeric values
                    last_3_sales = [float(x) if not pd.isna(x) else 0.0 for x in last_3_sales]

                    last_3_months_avg = round(series.iloc[-4:-1].mean(), 2) if len(series) >= 4 else round(series[-3:].mean(), 2) if len(series) >= 3 else round(series.mean(), 2) if not series.empty else 0
                    # Handle NaN values
                    if pd.isna(last_3_months_avg):
                        last_3_months_avg = 0.0

                    total_sales = round(series.sum(), 2)
                    if pd.isna(total_sales):
                        total_sales = 0.0
                    lead_time = self.lead_times.get(sku, 2)

                    safety_stock, velocity_category, safety_stock_months = self.calculate_enhanced_safety_stock(series, lead_time, sku)
                    reorder_point = self.calculate_enhanced_reorder_point(series, lead_time, safety_stock)

                    current_inventory = inventory.get(sku, 0)
                    po_quantity, urgency = self.calculate_enhanced_po_quantity(series, current_inventory, reorder_point, sku, safety_stock_months)

                    order_dates, order_qtys, arrival_dates = self.calculate_future_orders(forecast, current_inventory, reorder_point, lead_time, safety_stock_months, sku)
                    months_of_inventory = self.calculate_months_of_inventory(current_inventory, forecast)

                    sku_info = self.velocity_categories.get(sku, {})

                    if current_inventory <= 0:
                        stock_status = "OUT OF STOCK"
                    elif current_inventory <= reorder_point:
                        stock_status = "REORDER NOW"
                    elif last_3_months_avg > 0 and current_inventory / last_3_months_avg < 1:
                        stock_status = "LOW STOCK"
                    elif last_3_months_avg > 0 and current_inventory / last_3_months_avg > 6:
                        stock_status = "OVERSTOCK"
                    else:
                        stock_status = "NORMAL"

                    ## Shopify Tab    
                    # Create dynamic forecast row in desired column order
                    row_data = {
                        'SKU': str(sku),
                        'Product_Name': product_name,
                        'Category': category_name,
                        'Status': status_name,
                        'Launch_Date': launch_date.strftime('%Y-%m-%d') if launch_date and not pd.isna(launch_date) else '',
                        'Forecast_Method': method,  # <-- Now comes early
                        'Years_Since_Launch': years_since_launch if years_since_launch is not None else '',
                        'Current_Inventory': current_inventory,
                        'Stock_Status': stock_status,
                        'PO_Urgency': urgency,
                        'Recommended_PO_Qty': po_quantity,
                        'Next_Order_Date': order_dates[0],
                        'Next_Order_Qty': order_qtys[0],
                        'Next_Arrival_Date': arrival_dates[0],
                        'Months_of_Inventory': months_of_inventory,
                        'Velocity_Category': velocity_category,
                        'Safety_Stock_Months': safety_stock_months,
                        'Reorder_Point': reorder_point,
                        'Safety_Stock': safety_stock,
                    }

                    # Add forecast columns with month/year labels
                    for idx, month_label in enumerate(forecast_months):
                        row_data[f'Forecast_{month_label}'] = int(forecast.iloc[idx]) if idx < len(forecast) else 0

                    # Add remaining metrics (still before historical and channel)
                    row_data.update({
                        'Last_3_Months_Avg': last_3_months_avg,
                        'Total_Sales': total_sales,
                        'Growth_Rate': growth_rate if not pd.isna(growth_rate) else 0.0,
                        'Order_2_Date': order_dates[1],
                        'Order_2_Qty': order_qtys[1],
                        'Order_2_Arrival': arrival_dates[1],
                        'Order_3_Date': order_dates[2],
                        'Order_3_Qty': order_qtys[2],
                        'Order_3_Arrival': arrival_dates[2],
                        'Lead_Time': lead_time,
                        'Service_Level': f"{sku_info.get('service_level', 0.85)*100:.0f}%",
                        'Monthly_Velocity': round(sku_info.get('monthly_velocity', 0), 1) if not pd.isna(sku_info.get('monthly_velocity', 0)) else 0.0,
                        'Velocity_Rank': sku_info.get('rank', 999),
                    })

                    # Add historical months (just before Channel)
                    for idx, month_label in enumerate(historical_months):
                        if idx < len(last_3_sales):
                            value = int(last_3_sales[idx]) if not pd.isna(last_3_sales[idx]) else 0
                        else:
                            value = 0
                        row_data[month_label] = value

                    # Finally add Channel
                    row_data['Channel'] = channel

                    # Append to forecast
                    forecast_data.append(row_data)


                except Exception as e:
                    print(f"   Error processing SKU {sku}: {e}")
                    if i < 10:  # Show detailed errors for first 10 SKUs
                        import traceback
                        traceback.print_exc()
                    continue

            df = pd.DataFrame(forecast_data)

            # Additional filtering to ensure we only have mapped products
            if not df.empty:
                initial_count = len(df)
                df = df[df['Product_Name'] != 'Unknown']
                final_count = len(df)
                filtered_out = initial_count - final_count

                if filtered_out > 0:
                    print(f"   Filtered out {filtered_out} SKUs without product mappings")

            print(f"Generated {len(df)} forecasts for {channel} (only mapped products)")
            print(f"✅ Successfully mapped {products_found} product names")

            return df

        except Exception:
            return pd.DataFrame()
        
    def create_enhanced_forecast_shopify_faire_special(self, channel, inventory, product_info, product_category, product_status):
            try:
                print(f"Creating enhanced forecast for {channel}...")

                all_skus = set()
                all_skus.update(self.lead_times.keys())
                all_skus.update(inventory.keys())
                all_skus.update(product_info.keys())
                all_skus.update(self.data['SKU'].unique())
                all_skus.update(product_category.keys())
                all_skus.update(product_status.keys())

                print(f"Processing {len(all_skus)} SKUs for {channel}")

                # Generate historical month labels (last 3 months)
                current_date = pd.Timestamp.now()
                historical_months = []
                for i in range(3, 0, -1):  # 3, 2, 1 months ago
                    past_date = current_date - pd.DateOffset(months=i)
                    historical_months.append(past_date.strftime('%b_%Y'))

                # Generate forecast month labels
                forecast_months = []
                for i in range(8):
                    future_date = current_date + pd.DateOffset(months=i+1)
                    forecast_months.append(future_date.strftime('%b_%Y'))  # e.g., 'Jun_2025'

                print(f"Historical periods: {', '.join(historical_months)}")
                print(f"Forecast periods: {', '.join(forecast_months)}")

                forecast_data = []
                products_found = 0

                for i, sku in enumerate(all_skus):
                    try:
                        if i % 20 == 0:
                            print(f"   Processing SKU {i+1}/{len(all_skus)}: {sku}")

                        series = self.prepare_data(channel, sku)
                        forecast, method, seasonal_periods, seasonal_factors, growth_rate = self.generate_forecast(series)

                        # Use enhanced SKU lookup for product names and launch dates
                        product_name = self.smart_sku_lookup(sku, product_info)
                        
                        category_name = self.smart_sku_lookup(sku, product_category)
                        status_name = self.smart_sku_lookup(sku, product_status)

                        launch_date = self.smart_sku_lookup(sku, None, self.launch_dates)
                        years_since_launch = self.calculate_years_since_launch(sku)

                        # Debug: Show some matching attempts
                        if i < 5 or (i % 50 == 0):
                            print(f"   DEBUG SKU {sku}: Product = '{product_name}'")

                        if product_name != 'Unknown':
                            products_found += 1
                        else:
                            # Skip forecasts for SKUs without valid product mappings
                            if i % 100 == 0:  # Only log occasionally to avoid spam
                                print(f"   Skipping SKU {sku} - no product mapping found")
                            continue

                        # Get last 3 months sales (skip the most recent month to avoid incomplete data)
                        last_3_sales = series.iloc[-4:-1].tolist() if len(series) >= 4 else series.tail(3).tolist() if len(series) >= 3 else [0, 0, 0]
                        while len(last_3_sales) < 3:
                            last_3_sales.insert(0, 0)
                        last_3_sales = last_3_sales[-3:]  # Ensure exactly 3 values

                        # Ensure we have valid numeric values
                        last_3_sales = [float(x) if not pd.isna(x) else 0.0 for x in last_3_sales]

                        last_3_months_avg = round(series.iloc[-4:-1].mean(), 2) if len(series) >= 4 else round(series[-3:].mean(), 2) if len(series) >= 3 else round(series.mean(), 2) if not series.empty else 0
                        # Handle NaN values
                        if pd.isna(last_3_months_avg):
                            last_3_months_avg = 0.0

                        total_sales = round(series.sum(), 2)
                        if pd.isna(total_sales):
                            total_sales = 0.0
                        lead_time = self.lead_times.get(sku, 2)

                        safety_stock, velocity_category, safety_stock_months = self.calculate_enhanced_safety_stock(series, lead_time, sku)
                        reorder_point = self.calculate_enhanced_reorder_point(series, lead_time, safety_stock)

                        current_inventory = inventory.get(sku, 0)
                        po_quantity, urgency = self.calculate_enhanced_po_quantity(series, current_inventory, reorder_point, sku, safety_stock_months)

                        order_dates, order_qtys, arrival_dates = self.calculate_future_orders(forecast, current_inventory, reorder_point, lead_time, safety_stock_months, sku)
                        months_of_inventory = self.calculate_months_of_inventory(current_inventory, forecast)

                        sku_info = self.velocity_categories.get(sku, {})

                        if current_inventory <= 0:
                            stock_status = "OUT OF STOCK"
                        elif current_inventory <= reorder_point:
                            stock_status = "REORDER NOW"
                        elif last_3_months_avg > 0 and current_inventory / last_3_months_avg < 1:
                            stock_status = "LOW STOCK"
                        elif last_3_months_avg > 0 and current_inventory / last_3_months_avg > 6:
                            stock_status = "OVERSTOCK"
                        else:
                            stock_status = "NORMAL"

                        ## Shopify Faire Tab    
                        # Create dynamic forecast row in desired column order
                        row_data = {
                            'SKU': str(sku),
                            'Product_Name': product_name,
                            'Category': category_name,
                            'Status': status_name,
                            'Launch_Date': launch_date.strftime('%Y-%m-%d') if launch_date and not pd.isna(launch_date) else '',
                            'Forecast_Method': method,  # <-- Now comes early
                            'Years_Since_Launch': years_since_launch if years_since_launch is not None else '',
                            'Current_Inventory': current_inventory,
                            'Stock_Status': stock_status,
                            'PO_Urgency': urgency,
                            'Recommended_PO_Qty': po_quantity,
                            'Next_Order_Date': order_dates[0],
                            'Next_Order_Qty': order_qtys[0],
                            'Next_Arrival_Date': arrival_dates[0],
                            'Months_of_Inventory': months_of_inventory,
                            'Velocity_Category': velocity_category,
                            'Safety_Stock_Months': safety_stock_months,
                            'Reorder_Point': reorder_point,
                            'Safety_Stock': safety_stock,
                        }

                        # Add forecast columns with month/year labels
                        for idx, month_label in enumerate(forecast_months):
                            row_data[f'Forecast_{month_label}'] = int(forecast.iloc[idx]) if idx < len(forecast) else 0

                        # Add remaining metrics (still before historical and channel)
                        row_data.update({
                            'Last_3_Months_Avg': last_3_months_avg,
                            'Total_Sales': total_sales,
                            'Growth_Rate': growth_rate if not pd.isna(growth_rate) else 0.0,
                            'Order_2_Date': order_dates[1],
                            'Order_2_Qty': order_qtys[1],
                            'Order_2_Arrival': arrival_dates[1],
                            'Order_3_Date': order_dates[2],
                            'Order_3_Qty': order_qtys[2],
                            'Order_3_Arrival': arrival_dates[2],
                            'Lead_Time': lead_time,
                            'Service_Level': f"{sku_info.get('service_level', 0.85)*100:.0f}%",
                            'Monthly_Velocity': round(sku_info.get('monthly_velocity', 0), 1) if not pd.isna(sku_info.get('monthly_velocity', 0)) else 0.0,
                            'Velocity_Rank': sku_info.get('rank', 999),
                        })

                        # Add historical months (just before Channel)
                        for idx, month_label in enumerate(historical_months):
                            if idx < len(last_3_sales):
                                value = int(last_3_sales[idx]) if not pd.isna(last_3_sales[idx]) else 0
                            else:
                                value = 0
                            row_data[month_label] = value

                        # Finally add Channel
                        row_data['Channel'] = channel

                        # Append to forecast
                        forecast_data.append(row_data)


                    except Exception as e:
                        print(f"   Error processing SKU {sku}: {e}")
                        if i < 10:  # Show detailed errors for first 10 SKUs
                            import traceback
                            traceback.print_exc()
                        continue

                df = pd.DataFrame(forecast_data)

                # Additional filtering to ensure we only have mapped products
                if not df.empty:
                    initial_count = len(df)
                    df = df[df['Product_Name'] != 'Unknown']
                    final_count = len(df)
                    filtered_out = initial_count - final_count

                    if filtered_out > 0:
                        print(f"   Filtered out {filtered_out} SKUs without product mappings")

                print(f"Generated {len(df)} forecasts for {channel} (only mapped products)")
                print(f"✅ Successfully mapped {products_found} product names")

                return df

            except Exception:
                return pd.DataFrame()    
  
        
    def create_finance_cash_flow_forecast(self, combined_forecast):
        try:
            print("Creating finance cash flow forecast...")

            if combined_forecast.empty:
                return pd.DataFrame()

            finance_data = []
            current_date = pd.Timestamp.now()

            for _, row in combined_forecast.iterrows():
                sku = row['SKU']
                product_name = row['Product_Name']
                velocity_category = row['Velocity_Category']

                orders = []

                if row['PO_Urgency'] == 'HIGH - Below Reorder Point' and row['Recommended_PO_Qty'] > 0:
                    orders.append({
                        'order_date': current_date.strftime('%Y-%m-%d'),
                        'order_month': current_date.strftime('%Y-%m'),
                        'quantity': row['Recommended_PO_Qty'],
                        'urgency': 'IMMEDIATE'
                    })

                if row['Next_Order_Date'] and row['Next_Order_Qty'] > 0:
                    try:
                        order_date = pd.to_datetime(row['Next_Order_Date'])
                        if order_date.year == current_date.year:
                            orders.append({
                                'order_date': order_date.strftime('%Y-%m-%d'),
                                'order_month': order_date.strftime('%Y-%m'),
                                'quantity': row['Next_Order_Qty'],
                                'urgency': 'SCHEDULED'
                            })
                    except:
                        pass

                if row['Order_2_Date'] and row['Order_2_Qty'] > 0:
                    try:
                        order_date = pd.to_datetime(row['Order_2_Date'])
                        if order_date.year == current_date.year:
                            orders.append({
                                'order_date': order_date.strftime('%Y-%m-%d'),
                                'order_month': order_date.strftime('%Y-%m'),
                                'quantity': row['Order_2_Qty'],
                                'urgency': 'SCHEDULED'
                            })
                    except:
                        pass

                for order in orders:
                    finance_record = {
                        'Order_Month': order['order_month'],
                        'Order_Date': order['order_date'],
                        'SKU': sku,
                        'Product_Name': product_name,
                        'Velocity_Category': velocity_category,
                        'Order_Quantity': order['quantity'],
                        'Order_Urgency': order['urgency'],
                        'Unit_Cost': '',
                        'Total_Order_Value': '',
                        'Supplier': '',
                        'Payment_Terms': '',
                        'Expected_Payment_Date': '',
                        'Lead_Time': row['Lead_Time'],
                        'Safety_Stock_Months': row['Safety_Stock_Months'],
                        'Current_Inventory': row['Current_Inventory'],
                        'Months_of_Inventory': row['Months_of_Inventory'],
                    }

                    finance_data.append(finance_record)

            finance_df = pd.DataFrame(finance_data)

            if not finance_df.empty:
                finance_df['Order_Date_Sort'] = pd.to_datetime(finance_df['Order_Date'])
                finance_df = finance_df.sort_values(['Order_Date_Sort', 'Velocity_Category', 'SKU'])
                finance_df = finance_df.drop('Order_Date_Sort', axis=1)

                print(f"Created finance forecast: {len(finance_df)} orders planned")

            return finance_df

        except Exception:
            return pd.DataFrame()
        
    def create_enhanced_forecast_amazon_fbm_special(self, channel, inventory, product_info, product_category, product_status):
            try:
                print(f"Creating enhanced forecast for {channel}...")

                all_skus = set()
                all_skus.update(self.lead_times.keys())
                all_skus.update(inventory.keys())
                all_skus.update(product_info.keys())
                all_skus.update(self.data['SKU'].unique())
                all_skus.update(product_category.keys())
                all_skus.update(product_status.keys())

                print(f"Processing {len(all_skus)} SKUs for {channel}")

                # Generate historical month labels (last 3 months)
                current_date = pd.Timestamp.now()
                historical_months = []
                for i in range(3, 0, -1):  # 3, 2, 1 months ago
                    past_date = current_date - pd.DateOffset(months=i)
                    historical_months.append(past_date.strftime('%b_%Y'))

                # Generate forecast month labels
                forecast_months = []
                for i in range(8):
                    future_date = current_date + pd.DateOffset(months=i+1)
                    forecast_months.append(future_date.strftime('%b_%Y'))  # e.g., 'Jun_2025'

                print(f"Historical periods: {', '.join(historical_months)}")
                print(f"Forecast periods: {', '.join(forecast_months)}")

                forecast_data = []
                products_found = 0

                for i, sku in enumerate(all_skus):
                    try:
                        if i % 20 == 0:
                            print(f"   Processing SKU {i+1}/{len(all_skus)}: {sku}")

                        series = self.prepare_data(channel, sku)
                        forecast, method, seasonal_periods, seasonal_factors, growth_rate = self.generate_forecast(series)

                        # Use enhanced SKU lookup for product names and launch dates
                        product_name = self.smart_sku_lookup(sku, product_info)
                        
                        category_name = self.smart_sku_lookup(sku, product_category)
                        status_name = self.smart_sku_lookup(sku, product_status)

                        launch_date = self.smart_sku_lookup(sku, None, self.launch_dates)
                        years_since_launch = self.calculate_years_since_launch(sku)

                        # Debug: Show some matching attempts
                        if i < 5 or (i % 50 == 0):
                            print(f"   DEBUG SKU {sku}: Product = '{product_name}'")

                        if product_name != 'Unknown':
                            products_found += 1
                        else:
                            # Skip forecasts for SKUs without valid product mappings
                            if i % 100 == 0:  # Only log occasionally to avoid spam
                                print(f"   Skipping SKU {sku} - no product mapping found")
                            continue

                        # Get last 3 months sales (skip the most recent month to avoid incomplete data)
                        last_3_sales = series.iloc[-4:-1].tolist() if len(series) >= 4 else series.tail(3).tolist() if len(series) >= 3 else [0, 0, 0]
                        while len(last_3_sales) < 3:
                            last_3_sales.insert(0, 0)
                        last_3_sales = last_3_sales[-3:]  # Ensure exactly 3 values

                        # Ensure we have valid numeric values
                        last_3_sales = [float(x) if not pd.isna(x) else 0.0 for x in last_3_sales]

                        last_3_months_avg = round(series.iloc[-4:-1].mean(), 2) if len(series) >= 4 else round(series[-3:].mean(), 2) if len(series) >= 3 else round(series.mean(), 2) if not series.empty else 0
                        # Handle NaN values
                        if pd.isna(last_3_months_avg):
                            last_3_months_avg = 0.0

                        total_sales = round(series.sum(), 2)
                        if pd.isna(total_sales):
                            total_sales = 0.0
                        lead_time = self.lead_times.get(sku, 2)

                        safety_stock, velocity_category, safety_stock_months = self.calculate_enhanced_safety_stock(series, lead_time, sku)
                        reorder_point = self.calculate_enhanced_reorder_point(series, lead_time, safety_stock)

                        current_inventory = inventory.get(sku, 0)
                        po_quantity, urgency = self.calculate_enhanced_po_quantity(series, current_inventory, reorder_point, sku, safety_stock_months)

                        order_dates, order_qtys, arrival_dates = self.calculate_future_orders(forecast, current_inventory, reorder_point, lead_time, safety_stock_months, sku)
                        months_of_inventory = self.calculate_months_of_inventory(current_inventory, forecast)

                        sku_info = self.velocity_categories.get(sku, {})

                        if current_inventory <= 0:
                            stock_status = "OUT OF STOCK"
                        elif current_inventory <= reorder_point:
                            stock_status = "REORDER NOW"
                        elif last_3_months_avg > 0 and current_inventory / last_3_months_avg < 1:
                            stock_status = "LOW STOCK"
                        elif last_3_months_avg > 0 and current_inventory / last_3_months_avg > 6:
                            stock_status = "OVERSTOCK"
                        else:
                            stock_status = "NORMAL"

                        ## Amazon FBM Tab    
                        # Create dynamic forecast row in desired column order
                        row_data = {
                            'SKU': str(sku),
                            'Product_Name': product_name,
                            'Category': category_name,
                            'Status': status_name,
                            'Launch_Date': launch_date.strftime('%Y-%m-%d') if launch_date and not pd.isna(launch_date) else '',
                            'Forecast_Method': method,  # <-- Now comes early
                            'Years_Since_Launch': years_since_launch if years_since_launch is not None else '',
                            'Current_Inventory': current_inventory,
                            'Stock_Status': stock_status,
                            'PO_Urgency': urgency,
                            'Recommended_PO_Qty': po_quantity,
                            'Next_Order_Date': order_dates[0],
                            'Next_Order_Qty': order_qtys[0],
                            'Next_Arrival_Date': arrival_dates[0],
                            'Months_of_Inventory': months_of_inventory,
                            'Velocity_Category': velocity_category,
                            'Safety_Stock_Months': safety_stock_months,
                            'Reorder_Point': reorder_point,
                            'Safety_Stock': safety_stock,
                        }

                        # Add forecast columns with month/year labels
                        for idx, month_label in enumerate(forecast_months):
                            row_data[f'Forecast_{month_label}'] = int(forecast.iloc[idx]) if idx < len(forecast) else 0

                        # Add remaining metrics (still before historical and channel)
                        row_data.update({
                            'Last_3_Months_Avg': last_3_months_avg,
                            'Total_Sales': total_sales,
                            'Growth_Rate': growth_rate if not pd.isna(growth_rate) else 0.0,
                            'Order_2_Date': order_dates[1],
                            'Order_2_Qty': order_qtys[1],
                            'Order_2_Arrival': arrival_dates[1],
                            'Order_3_Date': order_dates[2],
                            'Order_3_Qty': order_qtys[2],
                            'Order_3_Arrival': arrival_dates[2],
                            'Lead_Time': lead_time,
                            'Service_Level': f"{sku_info.get('service_level', 0.85)*100:.0f}%",
                            'Monthly_Velocity': round(sku_info.get('monthly_velocity', 0), 1) if not pd.isna(sku_info.get('monthly_velocity', 0)) else 0.0,
                            'Velocity_Rank': sku_info.get('rank', 999),
                        })

                        # Add historical months (just before Channel)
                        for idx, month_label in enumerate(historical_months):
                            if idx < len(last_3_sales):
                                value = int(last_3_sales[idx]) if not pd.isna(last_3_sales[idx]) else 0
                            else:
                                value = 0
                            row_data[month_label] = value

                        # Finally add Channel
                        row_data['Channel'] = channel

                        # Append to forecast
                        forecast_data.append(row_data)


                    except Exception as e:
                        print(f"   Error processing SKU {sku}: {e}")
                        if i < 10:  # Show detailed errors for first 10 SKUs
                            import traceback
                            traceback.print_exc()
                        continue

                df = pd.DataFrame(forecast_data)

                # Additional filtering to ensure we only have mapped products
                if not df.empty:
                    initial_count = len(df)
                    df = df[df['Product_Name'] != 'Unknown']
                    final_count = len(df)
                    filtered_out = initial_count - final_count

                    if filtered_out > 0:
                        print(f"   Filtered out {filtered_out} SKUs without product mappings")

                print(f"Generated {len(df)} forecasts for {channel} (only mapped products)")
                print(f"✅ Successfully mapped {products_found} product names")

                return df

            except Exception:
                return pd.DataFrame()    
            
    def create_enhanced_forecast_walmart_fbm_special(self, channel, inventory, product_info, product_category, product_status):
            try:
                print(f"Creating enhanced forecast for {channel}...")

                all_skus = set()
                all_skus.update(self.lead_times.keys())
                all_skus.update(inventory.keys())
                all_skus.update(product_info.keys())
                all_skus.update(self.data['SKU'].unique())
                all_skus.update(product_category.keys())
                all_skus.update(product_status.keys())

                print(f"Processing {len(all_skus)} SKUs for {channel}")

                # Generate historical month labels (last 3 months)
                current_date = pd.Timestamp.now()
                historical_months = []
                for i in range(3, 0, -1):  # 3, 2, 1 months ago
                    past_date = current_date - pd.DateOffset(months=i)
                    historical_months.append(past_date.strftime('%b_%Y'))

                # Generate forecast month labels
                forecast_months = []
                for i in range(8):
                    future_date = current_date + pd.DateOffset(months=i+1)
                    forecast_months.append(future_date.strftime('%b_%Y'))  # e.g., 'Jun_2025'

                print(f"Historical periods: {', '.join(historical_months)}")
                print(f"Forecast periods: {', '.join(forecast_months)}")

                forecast_data = []
                products_found = 0

                for i, sku in enumerate(all_skus):
                    try:
                        if i % 20 == 0:
                            print(f"   Processing SKU {i+1}/{len(all_skus)}: {sku}")

                        series = self.prepare_data(channel, sku)
                        forecast, method, seasonal_periods, seasonal_factors, growth_rate = self.generate_forecast(series)

                        # Use enhanced SKU lookup for product names and launch dates
                        product_name = self.smart_sku_lookup(sku, product_info)
                        
                        category_name = self.smart_sku_lookup(sku, product_category)
                        status_name = self.smart_sku_lookup(sku, product_status)

                        launch_date = self.smart_sku_lookup(sku, None, self.launch_dates)
                        years_since_launch = self.calculate_years_since_launch(sku)

                        # Debug: Show some matching attempts
                        if i < 5 or (i % 50 == 0):
                            print(f"   DEBUG SKU {sku}: Product = '{product_name}'")

                        if product_name != 'Unknown':
                            products_found += 1
                        else:
                            # Skip forecasts for SKUs without valid product mappings
                            if i % 100 == 0:  # Only log occasionally to avoid spam
                                print(f"   Skipping SKU {sku} - no product mapping found")
                            continue

                        # Get last 3 months sales (skip the most recent month to avoid incomplete data)
                        last_3_sales = series.iloc[-4:-1].tolist() if len(series) >= 4 else series.tail(3).tolist() if len(series) >= 3 else [0, 0, 0]
                        while len(last_3_sales) < 3:
                            last_3_sales.insert(0, 0)
                        last_3_sales = last_3_sales[-3:]  # Ensure exactly 3 values

                        # Ensure we have valid numeric values
                        last_3_sales = [float(x) if not pd.isna(x) else 0.0 for x in last_3_sales]

                        last_3_months_avg = round(series.iloc[-4:-1].mean(), 2) if len(series) >= 4 else round(series[-3:].mean(), 2) if len(series) >= 3 else round(series.mean(), 2) if not series.empty else 0
                        # Handle NaN values
                        if pd.isna(last_3_months_avg):
                            last_3_months_avg = 0.0

                        total_sales = round(series.sum(), 2)
                        if pd.isna(total_sales):
                            total_sales = 0.0
                        lead_time = self.lead_times.get(sku, 2)

                        safety_stock, velocity_category, safety_stock_months = self.calculate_enhanced_safety_stock(series, lead_time, sku)
                        reorder_point = self.calculate_enhanced_reorder_point(series, lead_time, safety_stock)

                        current_inventory = inventory.get(sku, 0)
                        po_quantity, urgency = self.calculate_enhanced_po_quantity(series, current_inventory, reorder_point, sku, safety_stock_months)

                        order_dates, order_qtys, arrival_dates = self.calculate_future_orders(forecast, current_inventory, reorder_point, lead_time, safety_stock_months, sku)
                        months_of_inventory = self.calculate_months_of_inventory(current_inventory, forecast)

                        sku_info = self.velocity_categories.get(sku, {})

                        if current_inventory <= 0:
                            stock_status = "OUT OF STOCK"
                        elif current_inventory <= reorder_point:
                            stock_status = "REORDER NOW"
                        elif last_3_months_avg > 0 and current_inventory / last_3_months_avg < 1:
                            stock_status = "LOW STOCK"
                        elif last_3_months_avg > 0 and current_inventory / last_3_months_avg > 6:
                            stock_status = "OVERSTOCK"
                        else:
                            stock_status = "NORMAL"

                        ## Walmart FBM Tab    
                        # Create dynamic forecast row in desired column order
                        row_data = {
                            'SKU': str(sku),
                            'Product_Name': product_name,
                            'Category': category_name,
                            'Status': status_name,
                            'Launch_Date': launch_date.strftime('%Y-%m-%d') if launch_date and not pd.isna(launch_date) else '',
                            'Forecast_Method': method,  # <-- Now comes early
                            'Years_Since_Launch': years_since_launch if years_since_launch is not None else '',
                            'Current_Inventory': current_inventory,
                            'Stock_Status': stock_status,
                            'PO_Urgency': urgency,
                            'Recommended_PO_Qty': po_quantity,
                            'Next_Order_Date': order_dates[0],
                            'Next_Order_Qty': order_qtys[0],
                            'Next_Arrival_Date': arrival_dates[0],
                            'Months_of_Inventory': months_of_inventory,
                            'Velocity_Category': velocity_category,
                            'Safety_Stock_Months': safety_stock_months,
                            'Reorder_Point': reorder_point,
                            'Safety_Stock': safety_stock,
                        }

                        # Add forecast columns with month/year labels
                        for idx, month_label in enumerate(forecast_months):
                            row_data[f'Forecast_{month_label}'] = int(forecast.iloc[idx]) if idx < len(forecast) else 0

                        # Add remaining metrics (still before historical and channel)
                        row_data.update({
                            'Last_3_Months_Avg': last_3_months_avg,
                            'Total_Sales': total_sales,
                            'Growth_Rate': growth_rate if not pd.isna(growth_rate) else 0.0,
                            'Order_2_Date': order_dates[1],
                            'Order_2_Qty': order_qtys[1],
                            'Order_2_Arrival': arrival_dates[1],
                            'Order_3_Date': order_dates[2],
                            'Order_3_Qty': order_qtys[2],
                            'Order_3_Arrival': arrival_dates[2],
                            'Lead_Time': lead_time,
                            'Service_Level': f"{sku_info.get('service_level', 0.85)*100:.0f}%",
                            'Monthly_Velocity': round(sku_info.get('monthly_velocity', 0), 1) if not pd.isna(sku_info.get('monthly_velocity', 0)) else 0.0,
                            'Velocity_Rank': sku_info.get('rank', 999),
                        })

                        # Add historical months (just before Channel)
                        for idx, month_label in enumerate(historical_months):
                            if idx < len(last_3_sales):
                                value = int(last_3_sales[idx]) if not pd.isna(last_3_sales[idx]) else 0
                            else:
                                value = 0
                            row_data[month_label] = value

                        # Finally add Channel
                        row_data['Channel'] = channel

                        # Append to forecast
                        forecast_data.append(row_data)


                    except Exception as e:
                        print(f"   Error processing SKU {sku}: {e}")
                        if i < 10:  # Show detailed errors for first 10 SKUs
                            import traceback
                            traceback.print_exc()
                        continue

                df = pd.DataFrame(forecast_data)

                # Additional filtering to ensure we only have mapped products
                if not df.empty:
                    initial_count = len(df)
                    df = df[df['Product_Name'] != 'Unknown']
                    final_count = len(df)
                    filtered_out = initial_count - final_count

                    if filtered_out > 0:
                        print(f"   Filtered out {filtered_out} SKUs without product mappings")

                print(f"Generated {len(df)} forecasts for {channel} (only mapped products)")
                print(f"✅ Successfully mapped {products_found} product names")

                return df

            except Exception:
                return pd.DataFrame()    


def upload_excel_to_google_sheet(excel_buffer, sheet_id=None):
    import pandas as pd
    import numpy as np
    import gspread
    import time
    from google.oauth2.service_account import Credentials

    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    credentials = Credentials.from_service_account_file("credentials.json", scopes=scopes)
    gc = gspread.authorize(credentials)

    # ✅ Set your fixed Google Sheet ID here
    sheet_id = "1051NJelrnQGKwKDXWmaMiU1-fBgm4ZSd4s_G2-hJIcE"

    try:
        sheet = gc.open_by_key(sheet_id)
        print(f"🔁 Connected to Google Sheet: {sheet.title}")
    except Exception as e:
        print("❌ Failed to connect to Google Sheet:", e)
        raise

    try:
        # Read Excel from buffer
        excel_buffer.seek(0)
        all_sheets = pd.read_excel(excel_buffer, sheet_name=None, engine="openpyxl")

        # Existing sheet names
        existing_titles = [ws.title for ws in sheet.worksheets()]

        for i, (sheet_name, df) in enumerate(all_sheets.items()):
            df = df.replace([np.nan, np.inf, -np.inf], "").astype(str)

            # Reuse existing sheet if available
            if sheet_name in existing_titles:
                ws = sheet.worksheet(sheet_name)
                ws.clear()
            else:
                # Add new sheet with enough space
                ws = sheet.add_worksheet(title=sheet_name[:99], rows=str(len(df)+50), cols=str(len(df.columns)+10))

            # Push to Google Sheet
            try:
                ws.update([df.columns.tolist()] + df.values.tolist())
                print(f"✅ Updated sheet: {sheet_name}")
            except Exception as e:
                print(f"⚠️ Error updating '{sheet_name}': {e}")
            
            time.sleep(1)  # Add delay to avoid quota issues

        return f"https://docs.google.com/spreadsheets/d/{sheet_id}"

    except Exception as e:
        print("❌ Unexpected error while uploading to Google Sheets:", e)
        import traceback
        traceback.print_exc()
        return None
 
def upload_to_google_drive_from_buffer(buffer):
    # BASE_DIR = os.path.dirname(__file__)
    # SERVICE_ACCOUNT_FILE = os.path.join(BASE_DIR, "GoogleDriveAPIKey.json")
    SCOPES = ['https://www.googleapis.com/auth/drive']
    SHARED_DRIVE_ID = '0ANRBYKNxrAXaUk9PVA'
    FOLDER_ID = '0ANRBYKNxrAXaUk9PVA'
    FIXED_FILENAME = "Forecasting Excel Workbook Format.xlsx"

    # Handle service account credentials
    local_drive_key = os.path.join(BASE_DIR, "GoogleDriveAPIKey.json")

    if os.path.exists(local_drive_key):
        SERVICE_ACCOUNT_FILE = local_drive_key
        print(f"✅ Using local Google Drive credentials: {SERVICE_ACCOUNT_FILE}")
    elif "gcp_service_account_drive" in os.environ:
        creds_dict = json.loads(os.environ["gcp_service_account_drive"])
        with open("temp_service_account.json", "w") as f:
            json.dump(dict(creds_dict), f)
        SERVICE_ACCOUNT_FILE = "temp_service_account.json"
        print("✅ Using Google Drive credentials from Streamlit secrets")
    else:
        raise FileNotFoundError(
            "❌ No local Google Drive credentials or Streamlit secrets found."
        )

    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    drive_service = build('drive', 'v3', credentials=credentials)

    # Check if file with same name already exists
    query = f"'{FOLDER_ID}' in parents and name = '{FIXED_FILENAME}' and trashed = false"
    result = drive_service.files().list(
        q=query,
        fields="files(id)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
        corpora="drive",
        driveId=SHARED_DRIVE_ID
    ).execute()

    existing_files = result.get("files", [])

    # If exists, update it
    if existing_files:
        file_id = existing_files[0]['id']
        buffer.seek(0)
        media = MediaIoBaseUpload(buffer,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            resumable=True
        )
        updated_file = drive_service.files().update(
            fileId=file_id,
            media_body=media,
            supportsAllDrives=True
        ).execute()
        print(f"♻️ Existing file updated: {FIXED_FILENAME} (ID: {file_id})")
    else:
        buffer.seek(0)
        media = MediaIoBaseUpload(buffer,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            resumable=True
        )
        file_metadata = {
            'name': FIXED_FILENAME,
            'parents': [FOLDER_ID],
            'driveId': SHARED_DRIVE_ID
        }
        uploaded_file = drive_service.files().create(
            body=file_metadata,
            media_body=media,
            supportsAllDrives=True,
            fields='id'
        ).execute()
        file_id = uploaded_file.get('id')
        print(f"✅ New file uploaded: {FIXED_FILENAME} (ID: {file_id})")

    return file_id

         
def main():
    try:
        print("ENHANCED INVENTORY FORECASTING MODEL - COMPREHENSIVE VERSION")
        print("=" * 60)

        GOOGLE_SHEET_URL = "https://docs.google.com/spreadsheets/d/1ZYugDxWgvmwye_zYYZJ4lgnY8hwZYKljEjGOKT2Cens/edit?gid=2126602512#gid=2126602512"
        WEEKLY_SALES_URL = "https://docs.google.com/spreadsheets/d/16WVvbzcdzeeI4ZL4OFou_7DVM7UAHWUvXmpYiHzOUw0/edit?gid=1908752665#gid=1908752665"
        INVENTORY_URL = "https://docs.google.com/spreadsheets/d/1_j7eJi52Kq8RHvK6e0RPBRK8wJ0DXUOMj7Z7yZHlZzM/edit?gid=404505721#gid=404505721"
        USE_GOOGLE_SHEETS = True


        print("Loading data files...")

        historical_sales_path = os.path.join(BASE_DIR, "historical_sales.csv")
        historical_data = pd.read_csv(historical_sales_path, sep=',', dtype={'SKU': str}, encoding='utf-8')
        print(f"   Historical sales: {len(historical_data)} records")

        # Clean historical SKUs more thoroughly
        historical_data['SKU'] = historical_data['SKU'].astype(str).str.strip()
        print(f"   Sample historical SKUs: {historical_data['SKU'].unique()[:5]}")

        # MODIFIED SECTION - Load inventory from Google Sheets instead of CSV
        inventory = {}
        
        if USE_GOOGLE_SHEETS and GOOGLE_SHEETS_AVAILABLE:
            try:
                print(f"Current working directory: {os.getcwd()}")
                print("Looking for credentials file...")

                print(f"BASE_DIR: {BASE_DIR}")

                # Define credential paths FIRST
                credential_paths = [
                    os.path.join(BASE_DIR, "credentials.json"),
                    os.path.join(BASE_DIR, "service-account-key.json"),
                    os.path.join(BASE_DIR, "config", "credentials.json"),
                    os.path.join(BASE_DIR, "config", "service-account-key.json"),
                ]

                print("Checking paths:")
                for p in credential_paths:
                    print(" -", p, "| Exists:", os.path.exists(p))

                credentials_file = None
                for path in credential_paths:
                    if os.path.exists(path):
                        credentials_file = path
                        print(f"✅ Found local credentials file: {path}")
                        break

                # If no local file, try Streamlit Cloud secrets
                if not credentials_file and "gcp_service_account_sheets" in os.environ:
                    try:
                        creds_dict = json.loads(os.environ["gcp_service_account_sheets"])
                        with open("temp_credentials.json", "w") as f:
                            json.dump(creds_dict, f)
                        credentials_file = "temp_credentials.json"
                        print("✅ Loaded credentials from Render environment variable")
                    except Exception as e:
                        print("❌ Failed to parse credentials from environment variable:", e)

                # If still nothing, fall back to CSV
                if not credentials_file:
                    print("❌ No credentials file found locally or in secrets. Falling back to CSV...")
                    raise FileNotFoundError("No credentials file found")

                # Create Google Sheets connector
                gs_connector = GoogleSheetsConnector(credentials_file)



                # Get inventory data from Google Sheets
                print(f"\n📦 Loading inventory data from Google Sheets...")
                inventory = gs_connector.get_inventory_data(INVENTORY_URL)
                print(f"   Current inventory from Google Sheets: {len(inventory)} SKUs")

                # Get product info and lead times
                product_info, lead_times, launch_dates, product_category, product_status = gs_connector.get_product_data(GOOGLE_SHEET_URL)

                print(f"   Product info from Google Sheets: {len(product_info)} SKUs")
                print(f"   Lead times from Google Sheets: {len(lead_times)} SKUs")
                print(f"   Launch dates from Google Sheets: {len([d for d in launch_dates.values() if d is not None])} SKUs")

                # Step 1: Load and extend Amazon weekly
                print(f"\nLoading Amazon FBA weekly sales data...")
                amazon_weekly_df = gs_connector.get_amazon_fba_weekly_sales(WEEKLY_SALES_URL)

                if not amazon_weekly_df.empty:
                    amazon_weekly_monthly = gs_connector.convert_amazon_weekly_to_monthly(amazon_weekly_df)
                    amazon_extended = gs_connector.extend_historical_data_with_amazon_weekly(historical_data.copy(), amazon_weekly_monthly)
                    print(f"✅ Amazon FBA data extended")
                else:
                    amazon_extended = historical_data.copy()
                    print("⚠️ No Amazon FBA weekly sales data found")

                # Step 2: Load and extend Shopify weekly
                print(f"\nLoading Shopify Main weekly sales data...")
                shopify_weekly_df = gs_connector.get_shopify_main_weekly_sales(WEEKLY_SALES_URL)

                if not shopify_weekly_df.empty:
                    shopify_weekly_monthly = gs_connector.convert_shopify_weekly_to_monthly(shopify_weekly_df)
                    shopify_extended = gs_connector.extend_historical_data_with_shopify_weekly(historical_data.copy(), shopify_weekly_monthly)
                    print(f"✅ Shopify Main data extended")
                else:
                    shopify_extended = historical_data.copy()
                    print("⚠️ No Shopify Main weekly sales data found")

                ### Shopify Faire    

                print(f"\nLoading Shopify Faire weekly sales data...")
                shopify_faire_weekly_df = gs_connector.get_shopify_faire_weekly_sales(WEEKLY_SALES_URL)

                if not shopify_faire_weekly_df.empty:
                    shopify_faire_weekly_monthly = gs_connector.convert_shopify_faire_weekly_to_monthly(shopify_faire_weekly_df)
                    shopify_faire_extended = gs_connector.extend_historical_data_with_shopify_faire_weekly(historical_data.copy(), shopify_faire_weekly_monthly)
                    print(f"✅ Shopify Faire data extended")
                else:
                    shopify_faire_extended = historical_data.copy()
                    print("⚠️ No Shopify Faire weekly sales data found")

                ### Walmart FBM
                print(f"\nLoading Walmart FBM weekly sales data...")
                walmart_fbm_weekly_df = gs_connector.get_walmart_fbm_weekly_sales(WEEKLY_SALES_URL)

                if not walmart_fbm_weekly_df.empty:
                    walmart_fbm_weekly_monthly = gs_connector.convert_walmart_fbm_weekly_to_monthly(walmart_fbm_weekly_df)
                    walmart_fbm_extended = gs_connector.extend_historical_data_with_walmart_fbm_weekly(historical_data.copy(), walmart_fbm_weekly_monthly)
                    print(f"✅ Walmart FBM data extended")
                else:
                    walmart_fbm_extended = historical_data.copy()
                    print("⚠️ No Walmart FBM weekly sales data found")

                ### Amazon FBM
                print(f"\nLoading Amazon FBM weekly sales data...")
                amazon_fbm_weekly_df = gs_connector.get_amazon_fbm_weekly_sales(WEEKLY_SALES_URL)

                if not amazon_fbm_weekly_df.empty:
                    amazon_fbm_weekly_monthly = gs_connector.convert_amazon_fbm_weekly_to_monthly(amazon_fbm_weekly_df)
                    amazon_fbm_extended = gs_connector.extend_historical_data_with_amazon_fbm_weekly(historical_data.copy(), amazon_fbm_weekly_monthly)
                    print(f"✅ Amazon FBM data extended")
                else:
                    amazon_fbm_extended = historical_data.copy()
                    print("⚠️ No Amazon FBM weekly sales data found")

                # Step 3: Combine Amazon + Shopify extended data
                historical_data = pd.concat([amazon_extended, shopify_extended, shopify_faire_extended, walmart_fbm_extended, amazon_fbm_extended], ignore_index=True)
                historical_data = historical_data.drop_duplicates(subset=['SKU', 'Channel', 'Date'], keep='last')
                historical_data = historical_data.sort_values(by=['SKU', 'Channel', 'Date'])

                historical_skus = set(historical_data['SKU'].unique())
                google_skus = set(product_info.keys())

                print(f"\n🔍 ENHANCED SKU MATCHING DEBUG:")
                print(f"   Historical data has {len(historical_skus)} unique SKUs")
                print(f"   Google Sheets has {len(google_skus)} unique SKUs")

                # Test direct matches
                direct_matches = historical_skus.intersection(google_skus)
                print(f"   Direct matches: {len(direct_matches)}")
                print(f"   Sample direct matches: {list(direct_matches)[:5]}")

                # Test the lookup function directly
                print(f"   Testing smart_sku_lookup function:")
                test_skus = ['810128951111', '810128951128', '810128951203']  # From the sample
                for test_sku in test_skus:
                    direct_result = product_info.get(test_sku, 'NOT_FOUND_DIRECT')
                    print(f"     {test_sku} -> Direct: {direct_result}")

                    # Test if it's in the set
                    in_google_skus = test_sku in google_skus
                    print(f"     {test_sku} -> In google_skus: {in_google_skus}")

            except Exception as e:
                print(f"❌ Error loading from Google Sheets: {e}")
                print("   Falling back to local CSV files...")
                USE_GOOGLE_SHEETS = False
        else:
            USE_GOOGLE_SHEETS = False

        if not USE_GOOGLE_SHEETS:
            # Fall back to CSV for inventory
            inventory_path = os.path.join(BASE_DIR, "current_inventory.csv")
            inventory_df = pd.read_csv(inventory_path, sep=",", dtype={"SKU": str}, encoding="utf-8")
            inventory_df['SKU'] = inventory_df['SKU'].astype(str).str.strip()
            inventory = dict(zip(inventory_df['SKU'], inventory_df['Inventory']))
            print(f"   Current inventory from CSV: {len(inventory)} SKUs")

            lead_times_path = os.path.join(BASE_DIR, "lead_times.csv")
            lead_times_df = pd.read_csv(lead_times_path, sep=",", dtype={"SKU": str}, encoding="utf-8")            
            lead_times_df['SKU'] = lead_times_df['SKU'].astype(str).str.strip()
            lead_times = dict(zip(lead_times_df['SKU'], lead_times_df['LeadTime']))
            print(f"   Lead times from CSV: {len(lead_times)} SKUs")

            product_info_path = os.path.join(BASE_DIR, "product_info.csv")
            product_info_df = pd.read_csv(product_info_path, sep=",", dtype={"Unit UPC": str}, encoding="utf-8")
            if 'Unit UPC' in product_info_df.columns:
                product_info_df = product_info_df.rename(columns={'Unit UPC': 'SKU'})
            product_info_df['SKU'] = product_info_df['SKU'].astype(str).str.strip()
            product_info = dict(zip(product_info_df['SKU'], product_info_df['Product Name']))
            print(f"   Product info from CSV: {len(product_info)} SKUs")

            # Create empty launch_dates for CSV fallback
            launch_dates = {}
            print(f"   Launch dates: Not available in CSV mode")

        model = EnhancedForecastingModel(historical_data, lead_times, launch_dates, service_level=0.95)

        print("\nGenerating forecasts...")
        amazon_forecast = model.create_enhanced_forecast('Amazon', inventory, product_info, product_category, product_status)
        shopify_forecast = model.create_enhanced_forecast_shopify_special('Shopify', inventory, product_info, product_category, product_status)
        shopify_faire_forecast = model.create_enhanced_forecast_shopify_faire_special('Shopify Faire', inventory, product_info, product_category, product_status)
        amazon_fbm_forecast = model.create_enhanced_forecast_amazon_fbm_special('Amazonfbm', inventory, product_info, product_category, product_status)
        walmart_fbm_forecast = model.create_enhanced_forecast_walmart_fbm_special('Walmartfbm', inventory, product_info, product_category, product_status)

        print("Creating combined channel analysis...")
        combined_forecast = model.combine_channel_forecasts(amazon_forecast, shopify_forecast, shopify_faire_forecast, amazon_fbm_forecast, walmart_fbm_forecast)

        # Generate actionable insights
        print("Generating actionable insights...")
        insights = model.generate_actionable_insights(combined_forecast)
        executive_summary = model.create_executive_summary(combined_forecast, insights)
        priority_matrix = model.create_action_priority_matrix(combined_forecast)

        finance_forecast = model.create_finance_cash_flow_forecast(combined_forecast)

        timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M")
        data_source = "GoogleSheets" if USE_GOOGLE_SHEETS else "CSV"
        filename = f'enhanced_forecast_COMPREHENSIVE_{data_source}_{timestamp}.xlsx'

        print(f"Saving results to {filename}...")

        # Create BytesIO buffer for Excel file (works for both CLI and Streamlit)
        excel_buffer = BytesIO()

        with pd.ExcelWriter(excel_buffer, engine='xlsxwriter',
                           engine_kwargs={'options': {'nan_inf_to_errors': True}}) as writer:
            # Get the xlsxwriter workbook and worksheet objects
            workbook = writer.book

            # Define formats
            header_format = workbook.add_format({
                'bold': True,
                'text_wrap': True,
                'valign': 'vcenter',
                'align': 'center',
                'bg_color': '#D7E4BC',
                'border': 1,
                'font_size': 10
            })

            urgent_format = workbook.add_format({
                'bg_color': '#FFC7CE',
                'font_color': '#9C0006',
                'bold': True
            })

            warning_format = workbook.add_format({
                'bg_color': '#FFEB9C',
                'font_color': '#9C5700'
            })

            good_format = workbook.add_format({
                'bg_color': '#C6EFCE',
                'font_color': '#006100'
            })

            date_format = workbook.add_format({
                'num_format': 'yyyy-mm-dd',
                'align': 'center'
            })

            number_format = workbook.add_format({
                'num_format': '#,##0',
                'align': 'right'
            })

            decimal_format = workbook.add_format({
                'num_format': '#,##0.0',
                'align': 'right'
            })

            currency_format = workbook.add_format({
                'num_format': '$#,##0',
                'align': 'right'
            })

            text_format = workbook.add_format({
                'text_wrap': True,
                'valign': 'top'
            })

            # 1. EXECUTIVE SUMMARY SHEET (FIRST - MOST IMPORTANT)
            exec_summary_data = pd.DataFrame([
                ['INVENTORY PLANNING EXECUTIVE SUMMARY', ''],
                ['Report Date:', executive_summary['date']],
                ['', ''],
                ['IMMEDIATE ACTIONS REQUIRED', ''],
                ['Critical Actions (Today):', executive_summary['immediate_actions_required']],
                ['High Priority (This Week):', executive_summary['weekly_actions_required']],
                ['', ''],
                ['INVENTORY HEALTH', ''],
                ['Total SKUs:', executive_summary['total_skus']],
                ['At-Risk SKUs (<2 months):', executive_summary['at_risk_skus']],
                ['Overstock SKUs (>6 months):', executive_summary['overstock_skus']],
                ['', ''],
                ['FINANCIAL IMPACT', ''],
                ['Current Inventory Value:', f"${executive_summary['total_inventory_value']:,.0f}"],
                ['PO Value Needed (Immediate):', f"${executive_summary['total_po_value_needed']:,.0f}"],
                ['', ''],
                ['CASH FLOW PROJECTION', ''],
                ['Next 30 Days:', f"${executive_summary['cash_flow_30_days']:,.0f}"],
                ['Next 60 Days:', f"${executive_summary['cash_flow_60_days']:,.0f}"],
                ['Next 90 Days:', f"${executive_summary['cash_flow_90_days']:,.0f}"],
            ], columns=['Metric', 'Value'])

            exec_summary_data.to_excel(writer, sheet_name='📊 Executive Summary', index=False, header=False)
            worksheet = writer.sheets['📊 Executive Summary']

            # Format executive summary with proper column widths
            worksheet.set_column('A:A', 35)  # Metric column
            worksheet.set_column('B:B', 25)  # Value column

            # Format rows
            title_format = workbook.add_format({
                'bold': True,
                'font_size': 16,
                'align': 'center',
                'valign': 'vcenter',
                'bg_color': '#4472C4',
                'font_color': 'white',
                'border': 1
            })

            section_format = workbook.add_format({
                'bold': True,
                'font_size': 12,
                'bg_color': '#D7E4BC',
                'border': 1
            })

            value_format = workbook.add_format({
                'font_size': 11,
                'align': 'right'
            })

            # Apply formatting
            worksheet.merge_range(0, 0, 0, 1, 'INVENTORY PLANNING EXECUTIVE SUMMARY', title_format)
            worksheet.set_row(0, 30)

            # Format section headers
            for row in [3, 7, 12, 16]:
                if row < len(exec_summary_data):
                    worksheet.merge_range(row, 0, row, 1, exec_summary_data.iloc[row, 0], section_format)
                    worksheet.set_row(row, 25)

            # Format data rows
            for row in range(1, len(exec_summary_data)):
                if row not in [0, 3, 7, 12, 16] and exec_summary_data.iloc[row, 1]:
                    worksheet.write(row, 1, exec_summary_data.iloc[row, 1], value_format)

            # 2. IMMEDIATE ACTIONS SHEET
            immediate_actions_list = insights.get('immediate_actions', [])

            print(f"📌 immediate_actions count: {len(immediate_actions_list)}")

            if immediate_actions_list:
                immediate_df = pd.DataFrame(immediate_actions_list)
                immediate_df.to_excel(writer, sheet_name='🚨 IMMEDIATE ACTIONS', index=False, header=False, startrow=1)
                worksheet = writer.sheets['🚨 IMMEDIATE ACTIONS']

                # Write headers safely
                for col_num, column in enumerate(immediate_df.columns):
                    worksheet.write(0, col_num, column, header_format)

                # Set specific column widths
                col_widths = {
                    0: 12,  # Priority
                    1: 15,  # SKU
                    2: 35,  # Product
                    3: 40,  # Action
                    4: 30,  # Reason
                    5: 10,  # Quantity
                    6: 25,  # Impact
                    7: 35   # Contact
                }

                for col, width in col_widths.items():
                    if col < len(immediate_df.columns):
                        worksheet.set_column(col, col, width)

                # Apply formatting
                for row in range(1, len(immediate_df) + 1):
                    priority = immediate_df.iloc[row-1]['Priority']
                    fmt = urgent_format if priority == 'CRITICAL' else warning_format
                    for col in range(len(immediate_df.columns)):
                        worksheet.write(row, col, immediate_df.iloc[row-1, col], fmt)

                worksheet.freeze_panes(1, 0)

            else:
                print("⚠️ No immediate actions generated — skipping that sheet.")

            # 3. ACTION PRIORITY MATRIX
            if not priority_matrix.empty:
                priority_matrix.to_excel(writer, sheet_name='📋 Action Priority Matrix', index=False, header=False, startrow=1)
                worksheet = writer.sheets['📋 Action Priority Matrix']

                # Write headers
                for col_num, column in enumerate(priority_matrix.columns):
                    worksheet.write(0, col_num, column, header_format)

                # Set column widths
                matrix_widths = {
                    'SKU': 15,
                    'Product_Name': 35,
                    'Velocity_Category': 10,
                    'Months_of_Inventory': 12,
                    'Priority_Score': 10,
                    'Action_Priority': 12,
                    'Action_Timeline': 15,
                    'Recommended_Action': 40,
                    'Order_Quantity': 12,
                    'Current_Inventory': 12,
                    'Next_Month_Forecast': 12
                }

                for i, col in enumerate(priority_matrix.columns):
                    width = matrix_widths.get(col, 15)
                    worksheet.set_column(i, i, width)

                # Apply conditional formatting for priority levels
                for row in range(1, len(priority_matrix) + 1):
                    priority = priority_matrix.iloc[row-1]['Action_Priority']
                    if priority == 'IMMEDIATE':
                        row_format = urgent_format
                    elif priority == 'HIGH':
                        row_format = warning_format
                    else:
                        row_format = None

                    if row_format:
                        for col in range(len(priority_matrix.columns)):
                            worksheet.write(row, col, priority_matrix.iloc[row-1, col], row_format)

                worksheet.freeze_panes(1, 0)

            # 4. WEEKLY ACTIONS
            if insights.get('weekly_actions'):
                weekly_df = pd.DataFrame(insights.get('weekly_actions'))
                weekly_df.to_excel(writer, sheet_name='📅 Weekly Actions', index=False, header=False, startrow=1)
                worksheet = writer.sheets['📅 Weekly Actions']

                # Write headers and format
                for col_num, column in enumerate(weekly_df.columns):
                    worksheet.write(0, col_num, column, header_format)

                # Set column widths
                worksheet.set_column('A:A', 12)  # Priority
                worksheet.set_column('B:B', 15)  # SKU
                worksheet.set_column('C:C', 35)  # Product
                worksheet.set_column('D:D', 25)  # Action
                worksheet.set_column('E:E', 20)  # Reason
                worksheet.set_column('F:F', 10)  # Quantity
                worksheet.set_column('G:G', 12)  # By_Date
                worksheet.set_column('H:H', 30)  # Notes

                worksheet.freeze_panes(1, 0)

            # 5. RISK ANALYSIS
            if insights.get('risk_analysis'):
                risk_df = pd.DataFrame(insights.get('risk_analysis'))
                risk_df.to_excel(writer, sheet_name='⚠️ Risk Analysis', index=False, header=False, startrow=1)
                worksheet = writer.sheets['⚠️ Risk Analysis']

                # Write headers
                for col_num, column in enumerate(risk_df.columns):
                    worksheet.write(0, col_num, column, header_format)

                # Set column widths
                worksheet.set_column('A:A', 12)  # Risk_Level
                worksheet.set_column('B:B', 15)  # SKU
                worksheet.set_column('C:C', 35)  # Product
                worksheet.set_column('D:D', 35)  # Issue
                worksheet.set_column('E:E', 15)  # Potential_Loss
                worksheet.set_column('F:F', 40)  # Mitigation

                # Highlight high-risk items
                for row in range(1, len(risk_df) + 1):
                    if risk_df.iloc[row-1]['Risk_Level'] == 'HIGH':
                        for col in range(len(risk_df.columns)):
                            value = risk_df.iloc[row-1, col]
                            # Handle NaN/Inf values
                            if pd.isna(value):
                                value = ''
                            elif isinstance(value, (int, float)):
                                if value == float('inf'):
                                    value = 999999
                                elif value == float('-inf'):
                                    value = -999999
                            worksheet.write(row, col, value, warning_format)

                worksheet.freeze_panes(1, 0)

            # 7. COST OPTIMIZATION SHEET
            if insights.get('cost_optimization'):
                cost_df = pd.DataFrame(insights.get('cost_optimization'))
                cost_df.to_excel(writer, sheet_name='💡 Cost Optimization', index=False, header=False, startrow=1)
                worksheet = writer.sheets['💡 Cost Optimization']

                # Write headers
                for col_num, column in enumerate(cost_df.columns):
                    worksheet.write(0, col_num, column, header_format)

                # Set column widths
                worksheet.set_column('A:A', 15)  # SKU
                worksheet.set_column('B:B', 35)  # Product
                worksheet.set_column('C:C', 12)  # Current_Order
                worksheet.set_column('D:D', 30)  # Suggestion
                worksheet.set_column('E:E', 25)  # Savings
                worksheet.set_column('F:F', 35)  # Action

                worksheet.freeze_panes(1, 0)

            # 6. OPPORTUNITIES SHEET
            if insights.get('opportunities'):
                opp_df = pd.DataFrame(insights.get('opportunities'))
                opp_df.to_excel(writer, sheet_name='🎯 Opportunities', index=False, header=False, startrow=1)
                worksheet = writer.sheets['🎯 Opportunities']

                # Write headers
                for col_num, column in enumerate(opp_df.columns):
                    worksheet.write(0, col_num, column, header_format)

                # Set column widths
                worksheet.set_column('A:A', 12)  # Type
                worksheet.set_column('B:B', 15)  # SKU
                worksheet.set_column('C:C', 35)  # Product
                worksheet.set_column('D:D', 20)  # Trend
                worksheet.set_column('E:E', 25)  # Action
                worksheet.set_column('F:F', 30)  # Potential

                # Apply good formatting to all opportunity rows
                for row in range(1, len(opp_df) + 1):
                    for col in range(len(opp_df.columns)):
                        worksheet.write(row, col, opp_df.iloc[row-1, col], good_format)

                worksheet.freeze_panes(1, 0)

            # 8. MONTHLY ACTIONS SHEET
            if insights.get('monthly_actions'):
                monthly_df = pd.DataFrame(insights.get('monthly_actions'))
                monthly_df.to_excel(writer, sheet_name='📆 Monthly Actions', index=False, header=False, startrow=1)
                worksheet = writer.sheets['📆 Monthly Actions']

                # Write headers
                for col_num, column in enumerate(monthly_df.columns):
                    worksheet.write(0, col_num, column, header_format)

                # Set column widths
                worksheet.set_column('A:A', 15)  # SKU
                worksheet.set_column('B:B', 35)  # Product
                worksheet.set_column('C:C', 20)  # Action
                worksheet.set_column('D:D', 12)  # Order_Date
                worksheet.set_column('E:E', 10)  # Quantity
                worksheet.set_column('F:F', 25)  # Preparation
                worksheet.set_column('G:G', 15)  # Budget_Impact

                worksheet.freeze_panes(1, 0)

            # Function to format worksheet with proper column widths
            def format_worksheet(worksheet, df, sheet_name):
                # Write headers with formatting
                for col_num, column in enumerate(df.columns):
                    worksheet.write(0, col_num, column, header_format)

                # Define optimal column widths based on column names and content
                column_widths = {
                    'SKU': 15,
                    'Product_Name': 35,
                    'Product': 35,
                    'Launch_Date': 12,
                    'Years_Since_Launch': 10,
                    'Current_Inventory': 12,
                    'Stock_Status': 15,
                    'PO_Urgency': 25,
                    'Recommended_PO_Qty': 15,
                    'Next_Order_Date': 12,
                    'Next_Order_Qty': 12,
                    'Next_Arrival_Date': 12,
                    'Months_of_Inventory': 12,
                    'Velocity_Category': 10,
                    'Safety_Stock_Months': 12,
                    'Reorder_Point': 12,
                    'Safety_Stock': 12,
                    'Last_3_Months_Avg': 12,
                    'Total_Sales': 12,
                    'Growth_Rate': 10,
                    'Order_2_Date': 12,
                    'Order_2_Qty': 10,
                    'Order_2_Arrival': 12,
                    'Order_3_Date': 12,
                    'Order_3_Qty': 10,
                    'Order_3_Arrival': 12,
                    'Lead_Time': 10,
                    'Service_Level': 10,
                    'Monthly_Velocity': 12,
                    'Velocity_Rank': 10,
                    'Channel': 10,
                    'Forecast_Method': 20,
                    'Priority': 12,
                    'Action': 40,
                    'Reason': 30,
                    'Quantity': 10,
                    'Impact': 30,
                    'Contact': 35,
                    'By_Date': 12,
                    'Notes': 30,
                    'Risk_Level': 12,
                    'Issue': 35,
                    'Potential_Loss': 15,
                    'Mitigation': 40,
                    'Order_Month': 12,
                    'Order_Date': 12,
                    'Order_Quantity': 12,
                    'Order_Urgency': 15,
                    'Unit_Cost': 10,
                    'Total_Order_Value': 15,
                    'Supplier': 20,
                    'Payment_Terms': 15,
                    'Expected_Payment_Date': 15,
                    'Priority_Score': 10,
                    'Action_Priority': 12,
                    'Action_Timeline': 15,
                    'Recommended_Action': 35,
                    'Next_Month_Forecast': 12,
                    'Type': 12,
                    'Trend': 20,
                    'Potential': 30,
                    'Current_Order': 12,
                    'Suggestion': 30,
                    'Savings': 25,
                    'Preparation': 30,
                    'Budget_Impact': 15,
                    'Metric': 30,
                    'Value': 20,
                    'Excess_Units': 12,
                    'Excess_Value': 15,
                    'SKU_Count': 10,
                    'Inventory_Value': 15,
                    'Order_Value': 12,
                    'Arrival_Date': 12,
                    'Mapping_Status': 20
                }

                # Set column widths
                for i, col in enumerate(df.columns):
                    # Check for specific column names first
                    if col in column_widths:
                        width = column_widths[col]
                    # Check for forecast columns (dynamic month names)
                    elif col.startswith('Forecast_'):
                        width = 12
                    # Check for month columns (Jan_2025, etc.)
                    elif any(month in col for month in ['Jan_', 'Feb_', 'Mar_', 'Apr_', 'May_', 'Jun_', 'Jul_', 'Aug_', 'Sep_', 'Oct_', 'Nov_', 'Dec_']):
                        width = 10
                    # Check for Amazon/Shopify breakdown columns
                    elif col.startswith('Amazon_') or col.startswith('Shopify_'):
                        width = 12
                    # Default widths based on data type
                    else:
                        # Calculate based on content
                        max_len = len(str(col))
                        for idx, value in enumerate(df[col][:10]):  # Check first 10 rows
                            try:
                                value_len = len(str(value)) if pd.notna(value) else 0
                                max_len = max(max_len, value_len)
                            except:
                                pass
                        width = min(max(max_len + 2, 8), 40)

                    worksheet.set_column(i, i, width)

                # Apply data formatting based on column type
                for row_num in range(1, len(df) + 1):
                    for col_num, col_name in enumerate(df.columns):
                        value = df.iloc[row_num-1, col_num]

                        # Handle NaN/Inf values
                        if pd.isna(value):
                            value = ''
                        elif isinstance(value, (int, float)):
                            if value == float('inf'):
                                value = 999999
                            elif value == float('-inf'):
                                value = -999999
                            elif pd.isna(value):
                                value = 0

                        # Apply formatting based on column type
                        if 'Date' in col_name and value != '':
                            try:
                                # Convert to datetime for proper Excel date formatting
                                if isinstance(value, str) and value:
                                    date_value = pd.to_datetime(value)
                                    worksheet.write_datetime(row_num, col_num, date_value, date_format)
                                else:
                                    worksheet.write(row_num, col_num, value, text_format)
                            except:
                                worksheet.write(row_num, col_num, value, text_format)

                        elif col_name in ['Current_Inventory', 'Recommended_PO_Qty', 'Next_Order_Qty',
                                         'Order_2_Qty', 'Order_3_Qty', 'Safety_Stock', 'Reorder_Point',
                                         'Order_Quantity', 'Quantity', 'Excess_Units', 'SKU_Count'] or 'Forecast_' in col_name:
                            try:
                                if isinstance(value, (int, float)) and pd.notna(value):
                                    worksheet.write(row_num, col_num, value, number_format)
                                else:
                                    worksheet.write(row_num, col_num, value, text_format)
                            except:
                                worksheet.write(row_num, col_num, value, text_format)

                        elif col_name in ['Months_of_Inventory', 'Last_3_Months_Avg', 'Growth_Rate',
                                         'Monthly_Velocity', 'Years_Since_Launch', 'Safety_Stock_Months']:
                            try:
                                if isinstance(value, (int, float)) and pd.notna(value):
                                    worksheet.write(row_num, col_num, value, decimal_format)
                                else:
                                    worksheet.write(row_num, col_num, value, text_format)
                            except:
                                worksheet.write(row_num, col_num, value, text_format)

                        elif col_name in ['Total_Order_Value', 'Inventory_Value', 'Excess_Value',
                                         'Order_Value', 'Potential_Loss', 'Budget_Impact'] or 'Value' in col_name:
                            try:
                                if isinstance(value, (int, float)) and pd.notna(value):
                                    worksheet.write(row_num, col_num, value, currency_format)
                                else:
                                    worksheet.write(row_num, col_num, value, text_format)
                            except:
                                worksheet.write(row_num, col_num, value, text_format)

                        else:
                            worksheet.write(row_num, col_num, value, text_format)

                # Set row height for header
                worksheet.set_row(0, 25)

                # Freeze the header row and first two columns based on sheet type
                if 'Executive' not in sheet_name:  # Don't freeze executive summary
                    if 'All Forecasts' in sheet_name or 'Amazon' in sheet_name or 'Shopify' in sheet_name:
                        worksheet.freeze_panes(1, 2)  # Freeze first 2 columns
                    else:
                        worksheet.freeze_panes(1, 0)  # Just freeze header row

                # Add autofilter for data sheets
                if len(df) > 0 and 'Executive' not in sheet_name:
                    worksheet.autofilter(0, 0, len(df), len(df.columns) - 1)

            # Write and format remaining sheets (after the special sheets)
            if not combined_forecast.empty:
                combined_forecast.to_excel(writer, sheet_name='📈 All Forecasts', index=False, header=False, startrow=1)
                worksheet = writer.sheets['📈 All Forecasts']
                format_worksheet(worksheet, combined_forecast, '📈 All Forecasts')

                # Add conditional formatting for Stock Status
                status_col_idx = None
                for i, col in enumerate(combined_forecast.columns):
                    if col == 'Stock_Status':
                        status_col_idx = i
                        break

                if status_col_idx is not None:
                    for row in range(1, len(combined_forecast) + 1):
                        status = combined_forecast.iloc[row-1]['Stock_Status']
                        if status == 'OUT OF STOCK':
                            worksheet.write(row, status_col_idx, status, urgent_format)
                        elif status == 'REORDER NOW':
                            worksheet.write(row, status_col_idx, status, warning_format)
                        elif status == 'NORMAL':
                            worksheet.write(row, status_col_idx, status, good_format)

                # Apply conditional formatting for PO_Urgency
                urgency_col_idx = None
                for i, col in enumerate(combined_forecast.columns):
                    if col == 'PO_Urgency':
                        urgency_col_idx = i
                        break

                if urgency_col_idx is not None:
                    for row in range(1, len(combined_forecast) + 1):
                        urgency = combined_forecast.iloc[row-1]['PO_Urgency']
                        if 'HIGH' in urgency:
                            worksheet.write(row, urgency_col_idx, urgency, urgent_format)
                        elif 'MEDIUM' in urgency:
                            worksheet.write(row, urgency_col_idx, urgency, warning_format)

            if not finance_forecast.empty:
                finance_forecast.to_excel(writer, sheet_name='💰 Finance Cash Flow', index=False, header=False, startrow=1)
                worksheet = writer.sheets['💰 Finance Cash Flow']
                format_worksheet(worksheet, finance_forecast, '💰 Finance Cash Flow')

                # Apply specific formatting for finance sheet
                finance_widths = {
                    'Order_Month': 12,
                    'Order_Date': 12,
                    'SKU': 15,
                    'Product_Name': 35,
                    'Velocity_Category': 10,
                    'Order_Quantity': 12,
                    'Order_Urgency': 15,
                    'Unit_Cost': 10,
                    'Total_Order_Value': 15,
                    'Supplier': 20,
                    'Payment_Terms': 15,
                    'Expected_Payment_Date': 18,
                    'Lead_Time': 10,
                    'Safety_Stock_Months': 12,
                    'Current_Inventory': 12,
                    'Months_of_Inventory': 12
                }

                for i, col in enumerate(finance_forecast.columns):
                    if col in finance_widths:
                        worksheet.set_column(i, i, finance_widths[col])

                # Add a note for finance team
                note_text = "Please fill in Unit_Cost, Supplier, and Payment_Terms for accurate cash flow planning"
                worksheet.write(len(finance_forecast) + 3, 0, "Note:", header_format)
                worksheet.merge_range(len(finance_forecast) + 3, 1, len(finance_forecast) + 3, 6, note_text, text_format)

            if not amazon_forecast.empty:
                amazon_forecast.to_excel(writer, sheet_name='🛒 Amazon', index=False, header=False, startrow=1)
                worksheet = writer.sheets['🛒 Amazon']
                format_worksheet(worksheet, amazon_forecast, '🛒 Amazon')

            if not shopify_forecast.empty:
                shopify_forecast.to_excel(writer, sheet_name='🛍️ Shopify', index=False, header=False, startrow=1)
                worksheet = writer.sheets['🛍️ Shopify']
                format_worksheet(worksheet, shopify_forecast, '🛍️ Shopify')

            if not shopify_faire_forecast.empty:
                shopify_faire_forecast.to_excel(writer, sheet_name='🛍️ Shopify Faire', index=False, header=False, startrow=1)
                worksheet = writer.sheets['🛍️ Shopify Faire']
                format_worksheet(worksheet, shopify_faire_forecast, '🛍️ Shopify Faire')

            if not amazon_fbm_forecast.empty:
                amazon_fbm_forecast.to_excel(writer, sheet_name='🛍️ Amazon FBM', index=False, header=False, startrow=1)
                worksheet = writer.sheets['🛍️ Amazon FBM']
                format_worksheet(worksheet, amazon_fbm_forecast, '🛍️ Amazon FBM')

            if not walmart_fbm_forecast.empty:
                walmart_fbm_forecast.to_excel(writer, sheet_name='🛍️ Walmart FBM', index=False, header=False, startrow=1)
                worksheet = writer.sheets['🛍️ Walmart FBM']
                format_worksheet(worksheet, walmart_fbm_forecast, '🛍️ Walmart FBM')

            # Additional analysis sheets with proper formatting

            # Out of Stock Analysis
            if 'Stock_Status' in combined_forecast.columns:
                out_of_stock = combined_forecast[combined_forecast['Stock_Status'] == 'OUT OF STOCK']
                if not out_of_stock.empty:
                    out_of_stock.to_excel(writer, sheet_name='❌ Out of Stock', index=False, header=False, startrow=1)
                    worksheet = writer.sheets['❌ Out of Stock']
                    format_worksheet(worksheet, out_of_stock, '❌ Out of Stock')
                    # Highlight all rows as critical
                    for row in range(1, len(out_of_stock) + 1):
                        worksheet.set_row(row, None, urgent_format)

            # Reorder Now Analysis
            if 'Stock_Status' in combined_forecast.columns:
                reorder_now = combined_forecast[combined_forecast['Stock_Status'] == 'REORDER NOW']
                if not reorder_now.empty:
                    reorder_now.to_excel(writer, sheet_name='📦 Reorder Now', index=False, header=False, startrow=1)
                    worksheet = writer.sheets['📦 Reorder Now']
                    format_worksheet(worksheet, reorder_now, '📦 Reorder Now')
                    # Highlight all rows as warning
                    for row in range(1, len(reorder_now) + 1):
                        worksheet.set_row(row, None, warning_format)

            # Overstock Analysis
            if 'Months_of_Inventory' in combined_forecast.columns:
                overstock = combined_forecast[combined_forecast['Months_of_Inventory'] > 6]
                if not overstock.empty:
                    overstock_analysis = overstock[['SKU', 'Product_Name', 'Current_Inventory', 'Months_of_Inventory',
                                                   'Last_3_Months_Avg', 'Velocity_Category']].copy()
                    overstock_analysis['Excess_Units'] = overstock_analysis['Current_Inventory'] - (overstock_analysis['Last_3_Months_Avg'] * 3)
                    overstock_analysis['Excess_Value'] = overstock_analysis['Excess_Units'] * 30  # $30 cost assumption
                    overstock_analysis.to_excel(writer, sheet_name='📈 Overstock Analysis', index=False, header=False, startrow=1)
                    worksheet = writer.sheets['📈 Overstock Analysis']
                    format_worksheet(worksheet, overstock_analysis, '📈 Overstock Analysis')

            # Velocity Analysis
            if 'Velocity_Category' in combined_forecast.columns:
                velocity_summary = combined_forecast.groupby('Velocity_Category').agg({
                    'SKU': 'count',
                    'Current_Inventory': 'sum',
                    'Recommended_PO_Qty': 'sum'
                }).rename(columns={'SKU': 'SKU_Count'})
                velocity_summary['Inventory_Value'] = velocity_summary['Current_Inventory'] * 30
                velocity_summary.to_excel(writer, sheet_name='⚡ Velocity Analysis', startrow=1)
                worksheet = writer.sheets['⚡ Velocity Analysis']
                # Format velocity analysis
                worksheet.write(0, 0, 'Velocity_Category', header_format)
                worksheet.write(0, 1, 'SKU_Count', header_format)
                worksheet.write(0, 2, 'Current_Inventory', header_format)
                worksheet.write(0, 3, 'Recommended_PO_Qty', header_format)
                worksheet.write(0, 4, 'Inventory_Value', header_format)
                worksheet.set_column('A:A', 15)
                worksheet.set_column('B:B', 12)
                worksheet.set_column('C:C', 15)
                worksheet.set_column('D:D', 18)
                worksheet.set_column('E:E', 15)

            # Monthly Order Schedule
            order_schedule = []
            for _, row in combined_forecast.iterrows():
                if row['Next_Order_Date']:
                    order_schedule.append({
                        'Order_Date': row['Next_Order_Date'],
                        'SKU': row['SKU'],
                        'Product': row['Product_Name'],
                        'Quantity': row['Next_Order_Qty'],
                        'Lead_Time': row['Lead_Time'],
                        'Arrival_Date': row['Next_Arrival_Date']
                    })
                if row['Order_2_Date']:
                    order_schedule.append({
                        'Order_Date': row['Order_2_Date'],
                        'SKU': row['SKU'],
                        'Product': row['Product_Name'],
                        'Quantity': row['Order_2_Qty'],
                        'Lead_Time': row['Lead_Time'],
                        'Arrival_Date': row['Order_2_Arrival']
                    })

            if order_schedule:
                schedule_df = pd.DataFrame(order_schedule)
                schedule_df['Order_Date'] = pd.to_datetime(schedule_df['Order_Date'])
                schedule_df = schedule_df.sort_values('Order_Date')
                schedule_df['Order_Value'] = schedule_df['Quantity'] * 30
                # Convert back to string for Excel
                schedule_df['Order_Date'] = schedule_df['Order_Date'].dt.strftime('%Y-%m-%d')
                schedule_df.to_excel(writer, sheet_name='📅 Order Schedule', index=False, header=False, startrow=1)
                worksheet = writer.sheets['📅 Order Schedule']
                # Write headers
                headers = ['Order_Date', 'SKU', 'Product', 'Quantity', 'Lead_Time', 'Arrival_Date', 'Order_Value']
                for col_num, header in enumerate(headers):
                    worksheet.write(0, col_num, header, header_format)
                # Set column widths
                worksheet.set_column('A:A', 12)  # Order_Date
                worksheet.set_column('B:B', 15)  # SKU
                worksheet.set_column('C:C', 35)  # Product
                worksheet.set_column('D:D', 10)  # Quantity
                worksheet.set_column('E:E', 10)  # Lead_Time
                worksheet.set_column('F:F', 12)  # Arrival_Date
                worksheet.set_column('G:G', 12)  # Order_Value
                # Format data
                for row_num in range(1, len(schedule_df) + 1):
                    for col_num in range(len(headers)):
                        value = schedule_df.iloc[row_num-1, col_num]
                        if headers[col_num] == 'Order_Value':
                            worksheet.write(row_num, col_num, value, currency_format)
                        elif headers[col_num] in ['Quantity', 'Lead_Time']:
                            worksheet.write(row_num, col_num, value, number_format)
                        else:
                            worksheet.write(row_num, col_num, value, text_format)
                worksheet.freeze_panes(1, 0)
                worksheet.autofilter(0, 0, len(schedule_df), len(headers) - 1)

            # Create a mapping report to show which SKUs were matched vs filtered out
            if not combined_forecast.empty:
                # Show only mapped products in the main report
                mapping_report = combined_forecast[['SKU', 'Product_Name', 'Launch_Date', 'Years_Since_Launch']].copy()
                mapping_report['Mapping_Status'] = 'SUCCESSFULLY_MAPPED'
                mapping_report.to_excel(writer, sheet_name='🔗 Mapped Products', index=False, header=False, startrow=1)
                worksheet = writer.sheets['🔗 Mapped Products']
                format_worksheet(worksheet, mapping_report, '🔗 Mapped Products')

        # For CLI execution - save to file
        try:
            import sys
            is_streamlit = "streamlit" in sys.modules
        except:
            is_streamlit = False
            
        if not is_streamlit:
            # Save to file for CLI execution
            with open(filename, 'wb') as f:
                f.write(excel_buffer.getvalue())

        print(f"\n🎉 SUCCESS! Comprehensive enhanced forecasting complete!")
        print(f"Results saved to: {filename}")
        print(f"Total mapped SKUs processed: {len(combined_forecast)}")
        print(f"Data source: {'Google Sheets (with Inventory from ' + INVENTORY_URL + ')' if USE_GOOGLE_SHEETS else 'Local CSV files'}")

        # ACTIONABLE INSIGHTS SUMMARY
        print("\n" + "="*60)
        print("📊 ACTIONABLE INSIGHTS SUMMARY")
        print("="*60)

        # Immediate Actions
        if insights.get('immediate_actions'):
            print(f"\n🚨 IMMEDIATE ACTIONS REQUIRED ({len(insights.get('immediate_actions'))} items):")
            print("-" * 50)
            for action in insights.get('immediate_actions')[:5]:  # Show top 5
                print(f"• {action['Product']} (SKU: {action['SKU']})")
                print(f"  ACTION: {action['Action']}")
                print(f"  REASON: {action['Reason']}")
                print(f"  QUANTITY: {action['Quantity']} units")
                print(f"  IMPACT: {action['Impact']}")
                print()

        # Risk Analysis
        high_risks = [r for r in insights.get('risk_analysis', []) if r['Risk_Level'] == 'HIGH']

        if high_risks:
            print(f"\n⚠️ HIGH RISK ITEMS ({len(high_risks)} items):")
            print("-" * 50)
            for risk in high_risks[:3]:
                print(f"• {risk['Product']} - {risk['Issue']}")
                print(f"  POTENTIAL LOSS: {risk['Potential_Loss']}")
                print(f"  MITIGATION: {risk['Mitigation']}")
                print()

        # Cash Flow Summary
        print("\n💰 CASH FLOW REQUIREMENTS:")
        print("-" * 50)
        print(f"Next 30 days: ${executive_summary['cash_flow_30_days']:,.0f}")
        print(f"Next 60 days: ${executive_summary['cash_flow_60_days']:,.0f}")
        print(f"Next 90 days: ${executive_summary['cash_flow_90_days']:,.0f}")
        print(f"Total PO Value Needed: ${executive_summary['total_po_value_needed']:,.0f}")

        # Inventory Health
        print("\n📦 INVENTORY HEALTH STATUS:")
        print("-" * 50)
        print(f"At-Risk SKUs (<2 months inventory): {executive_summary['at_risk_skus']}")
        print(f"Overstock SKUs (>6 months inventory): {executive_summary['overstock_skus']}")
        print(f"Total Inventory Value: ${executive_summary['total_inventory_value']:,.0f}")

        # Top Actions by Priority
        if not priority_matrix.empty:
            immediate_actions = priority_matrix[priority_matrix['Action_Priority'] == 'IMMEDIATE']
            high_actions = priority_matrix[priority_matrix['Action_Priority'] == 'HIGH']

            print(f"\n📋 ACTION PRIORITY SUMMARY:")
            print("-" * 50)
            print(f"IMMEDIATE actions required: {len(immediate_actions)} SKUs")
            print(f"HIGH priority actions: {len(high_actions)} SKUs")

            if not immediate_actions.empty:
                print("\nTop 3 IMMEDIATE priorities:")
                for _, row in immediate_actions.head(3).iterrows():
                    print(f"• {row['Product_Name']} - {row['Recommended_Action']}")

        # Quick Win Opportunities
        if insights.get('opportunities'):
            print(f"\n🎯 GROWTH OPPORTUNITIES ({len(insights.get('opportunities'))} items):")
            print("-" * 50)
            for opp in insights.get('opportunities')[:3]:
                print(f"• {opp['Product']} - {opp['Trend']}")
                print(f"  ACTION: {opp['Action']}")
                print()

        print("\n" + "="*60)
        print("✅ NEXT STEPS:")
        print("1. Review '🚨 IMMEDIATE ACTIONS' sheet and place urgent orders TODAY")
        print("2. Check '📊 Executive Summary' for overall inventory health")
        print("3. Use '📋 Action Priority Matrix' to plan this week's activities")
        print("4. Review '⚠️ Risk Analysis' to prevent stockouts")
        print("5. Share 'Finance Cash Flow' sheet with finance team for budget planning")
        print("="*60)

        # Enhanced summary of product mapping - only show mapped products
        if not combined_forecast.empty and 'Product_Name' in combined_forecast.columns:
            total_processed_skus = len(combined_forecast)

            print(f"\n📊 PRODUCT MAPPING SUMMARY:")
            print(f"   SKUs with valid product mappings: {total_processed_skus}")
            print(f"   Mapping success rate: 100% (only mapped products included)")
            print(f"   Inventory data source: Google Sheets" if USE_GOOGLE_SHEETS else "Local CSV")

            # Show some examples of successfully mapped products
            if total_processed_skus > 0:
                sample_mapped = combined_forecast[['SKU', 'Product_Name']].head(5)
                print(f"\n✅ Successfully mapped products (sample):")
                for _, row in sample_mapped.iterrows():
                    print(f"   {row['SKU']} -> {row['Product_Name']}")

        else:
            print(f"\n⚠️  No forecasts generated - check data sources and UPC mapping")

        print("\n✅ Ready for inventory planning decisions!")

        # Return the Excel buffer for Streamlit
        excel_buffer.seek(0)

        # Upload to Google Sheets before returning
        upload_excel_to_google_sheet(excel_buffer)

        # Upload to Google Drive (NEW)
        drive_file_id = upload_to_google_drive_from_buffer(excel_buffer)

        return excel_buffer, filename, drive_file_id


    except FileNotFoundError as e:
        print(f"File not found: {e}")
        print("   Make sure all CSV files are in the current directory:")
        print("   - historical_sales.csv")
        print("   - current_inventory.csv (if not using Google Sheets)")
        print("   - lead_times.csv")
        print("   - product_info.csv")
        return None, None

    except Exception as e:
        print(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return None, None

## Streamlit UI
# --- Import required modules ---

# --- Set page config ---
st.set_page_config(
    page_title="Herbal Goodness | AI Forecasting", 
    layout="wide",
    page_icon="🔮"
)

# --- Innovative Digital Styling ---
st.markdown("""
    <style>
        /* Import futuristic fonts */
        @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@300;400;500;600;700&family=JetBrains+Mono:wght@300;400;500;600&display=swap');
        
        /* Global reset and base styling */
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        /* Main app background with animated gradient */
        .stApp {
            background: linear-gradient(135deg, 
                #0a0f1c 0%, 
                #1a2332 25%, 
                #0d2439 50%, 
                #1e3a52 75%, 
                #0f1419 100%);
            background-size: 400% 400%;
            animation: gradientShift 15s ease infinite;
            min-height: 100vh;
            position: relative;
        }
        
        /* Animated background */
        @keyframes gradientShift {
            0% { background-position: 0% 50%; }
            50% { background-position: 100% 50%; }
            100% { background-position: 0% 50%; }
        }
        
        /* Floating particles effect */
        .stApp::before {
            content: '';
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background-image: 
                radial-gradient(circle at 20% 20%, rgba(64, 224, 208, 0.1) 0%, transparent 50%),
                radial-gradient(circle at 80% 80%, rgba(135, 206, 235, 0.1) 0%, transparent 50%),
                radial-gradient(circle at 40% 60%, rgba(144, 238, 144, 0.05) 0%, transparent 50%);
            pointer-events: none;
            z-index: 0;
        }
        
        /* Main content container */
        .main-content {
            position: relative;
            z-index: 1;
        }
        
        /* Futuristic header with glassmorphism */
        .header-container {
            background: linear-gradient(135deg, 
                rgba(64, 224, 208, 0.1) 0%, 
                rgba(135, 206, 235, 0.15) 50%, 
                rgba(144, 238, 144, 0.1) 100%);
            backdrop-filter: blur(20px);
            border: 1px solid rgba(255, 255, 255, 0.1);
            border-radius: 24px;
            padding: 3rem;
            margin: 2rem 0;
            position: relative;
            overflow: hidden;
            box-shadow: 
                0 8px 32px rgba(0, 0, 0, 0.3),
                inset 0 1px 0 rgba(255, 255, 255, 0.1);
        }
        
        .header-container::before {
            content: '';
            position: absolute;
            top: 0;
            left: -100%;
            width: 100%;
            height: 100%;
            background: linear-gradient(90deg, 
                transparent, 
                rgba(64, 224, 208, 0.1), 
                transparent);
            animation: shimmer 3s infinite;
        }
        
        @keyframes shimmer {
            0% { left: -100%; }
            100% { left: 100%; }
        }
        
        /* Logo and branding section */
        .brand-section {
            display: flex;
            align-items: center;
            gap: 2rem;
            margin-bottom: 1.5rem;
        }
        
        .logo-placeholder {
            width: 80px;
            height: 80px;
            background: linear-gradient(135deg, #40e0d0, #87ceeb);
            border-radius: 20px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 2.5rem;
            box-shadow: 0 8px 32px rgba(64, 224, 208, 0.3);
            animation: float 3s ease-in-out infinite;
        }
        
        @keyframes float {
            0%, 100% { transform: translateY(0px); }
            50% { transform: translateY(-10px); }
        }
        
        .brand-text {
            flex: 1;
        }
        
        .company-name {
            font-family: 'Space Grotesk', sans-serif;
            font-size: 3.5rem;
            font-weight: 700;
            background: linear-gradient(135deg, #40e0d0, #87ceeb, #90ee90);
            background-clip: text;
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            margin: 0;
            text-shadow: 0 0 30px rgba(64, 224, 208, 0.5);
        }
        
        .tagline {
            font-family: 'JetBrains Mono', monospace;
            font-size: 1.1rem;
            color: rgba(255, 255, 255, 0.7);
            margin-top: 0.5rem;
            letter-spacing: 1px;
        }
        
        /* Holographic cards */
        .holo-card {
            background: linear-gradient(135deg, 
                rgba(255, 255, 255, 0.05) 0%, 
                rgba(64, 224, 208, 0.1) 50%, 
                rgba(135, 206, 235, 0.05) 100%);
            backdrop-filter: blur(15px);
            border: 1px solid rgba(255, 255, 255, 0.1);
            border-radius: 20px;
            padding: 2.5rem;
            margin: 2rem 0;
            position: relative;
            overflow: hidden;
            transition: all 0.4s cubic-bezier(0.23, 1, 0.320, 1);
            box-shadow: 
                0 10px 40px rgba(0, 0, 0, 0.2),
                inset 0 1px 0 rgba(255, 255, 255, 0.1);
        }
        
        .holo-card:hover {
            transform: translateY(-5px);
            box-shadow: 
                0 20px 60px rgba(0, 0, 0, 0.3),
                0 0 50px rgba(64, 224, 208, 0.2);
            border-color: rgba(64, 224, 208, 0.3);
        }
        
        .holo-card::after {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            height: 1px;
            background: linear-gradient(90deg, 
                transparent, 
                rgba(64, 224, 208, 0.8), 
                rgba(135, 206, 235, 0.8), 
                transparent);
        }
        
        /* Section headers with neon effect */
        .section-title {
            font-family: 'Space Grotesk', sans-serif;
            font-size: 1.8rem;
            font-weight: 600;
            color: #ffffff;
            margin-bottom: 1.5rem;
            display: flex;
            align-items: center;
            gap: 1rem;
            position: relative;
        }
        
        .section-title::after {
            content: '';
            flex: 1;
            height: 1px;
            background: linear-gradient(90deg, 
                rgba(64, 224, 208, 0.5), 
                transparent);
        }
        
        /* Futuristic buttons */
        .quantum-btn {
            background: linear-gradient(135deg, 
                rgba(64, 224, 208, 0.2) 0%, 
                rgba(135, 206, 235, 0.3) 50%, 
                rgba(144, 238, 144, 0.2) 100%);
            border: 2px solid transparent;
            background-clip: padding-box;
            border-radius: 16px;
            padding: 1.2rem 2.5rem;
            font-family: 'Space Grotesk', sans-serif;
            font-size: 1.1rem;
            font-weight: 600;
            color: #ffffff;
            cursor: pointer;
            position: relative;
            overflow: hidden;
            transition: all 0.4s cubic-bezier(0.23, 1, 0.320, 1);
            text-transform: uppercase;
            letter-spacing: 1px;
            box-shadow: 
                0 8px 32px rgba(64, 224, 208, 0.2),
                inset 0 1px 0 rgba(255, 255, 255, 0.1);
            width: 100%;
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 0.75rem;
        }
        
        .quantum-btn::before {
            content: '';
            position: absolute;
            top: 0;
            left: -100%;
            width: 100%;
            height: 100%;
            background: linear-gradient(90deg, 
                transparent, 
                rgba(255, 255, 255, 0.2), 
                transparent);
            transition: left 0.6s;
        }
        
        .quantum-btn:hover {
            transform: translateY(-3px) scale(1.02);
            box-shadow: 
                0 15px 50px rgba(64, 224, 208, 0.4),
                0 0 30px rgba(135, 206, 235, 0.3);
            border-color: rgba(64, 224, 208, 0.6);
        }
        
        .quantum-btn:hover::before {
            left: 100%;
        }
        
        /* Secondary action buttons */
        .neural-btn {
            background: rgba(255, 255, 255, 0.05);
            border: 1px solid rgba(64, 224, 208, 0.3);
            border-radius: 12px;
            padding: 1rem 1.8rem;
            font-family: 'Space Grotesk', sans-serif;
            font-size: 0.95rem;
            font-weight: 500;
            color: #40e0d0;
            cursor: pointer;
            transition: all 0.3s cubic-bezier(0.23, 1, 0.320, 1);
            text-decoration: none;
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 0.5rem;
            position: relative;
            overflow: hidden;
            width: 100%;
        }
        
        .neural-btn:hover {
            background: linear-gradient(135deg, 
                rgba(64, 224, 208, 0.1), 
                rgba(135, 206, 235, 0.1));
            border-color: rgba(64, 224, 208, 0.8);
            transform: translateY(-2px);
            box-shadow: 0 10px 30px rgba(64, 224, 208, 0.2);
            color: #ffffff;
            text-decoration: none;
        }
        
        /* Override Streamlit button styles */
        .stButton > button {
            background: linear-gradient(135deg, 
                rgba(64, 224, 208, 0.2) 0%, 
                rgba(135, 206, 235, 0.3) 50%, 
                rgba(144, 238, 144, 0.2) 100%) !important;
            border: 2px solid rgba(64, 224, 208, 0.4) !important;
            border-radius: 16px !important;
            padding: 1.2rem 2.5rem !important;
            font-family: 'Space Grotesk', sans-serif !important;
            font-size: 1.1rem !important;
            font-weight: 600 !important;
            color: #ffffff !important;
            text-transform: uppercase !important;
            letter-spacing: 1px !important;
            transition: all 0.4s cubic-bezier(0.23, 1, 0.320, 1) !important;
            width: 100% !important;
            box-shadow: 0 8px 32px rgba(64, 224, 208, 0.2) !important;
        }
        
        .stButton > button:hover {
            transform: translateY(-3px) scale(1.02) !important;
            box-shadow: 0 15px 50px rgba(64, 224, 208, 0.4) !important;
            border-color: rgba(64, 224, 208, 0.8) !important;
        }
        
        /* Download button styling */
        .stDownloadButton > button {
            background: rgba(255, 255, 255, 0.05) !important;
            border: 1px solid rgba(64, 224, 208, 0.3) !important;
            border-radius: 12px !important;
            padding: 1rem 1.8rem !important;
            font-family: 'Space Grotesk', sans-serif !important;
            font-size: 0.95rem !important;
            font-weight: 500 !important;
            color: #40e0d0 !important;
            transition: all 0.3s cubic-bezier(0.23, 1, 0.320, 1) !important;
            width: 100% !important;
        }
        
        .stDownloadButton > button:hover {
            background: linear-gradient(135deg, 
                rgba(64, 224, 208, 0.1), 
                rgba(135, 206, 235, 0.1)) !important;
            border-color: rgba(64, 224, 208, 0.8) !important;
            transform: translateY(-2px) !important;
            box-shadow: 0 10px 30px rgba(64, 224, 208, 0.2) !important;
            color: #ffffff !important;
        }
        
        /* Progress bar styling */
        .stProgress > div > div > div > div {
            background: linear-gradient(90deg, 
                #40e0d0 0%, 
                #87ceeb 50%, 
                #90ee90 100%) !important;
            box-shadow: 0 0 20px rgba(64, 224, 208, 0.5) !important;
        }
        
        /* Success messages */
        .stSuccess {
            background: linear-gradient(135deg, 
                rgba(144, 238, 144, 0.1), 
                rgba(64, 224, 208, 0.1)) !important;
            border: 1px solid rgba(144, 238, 144, 0.3) !important;
            border-radius: 12px !important;
            color: #90ee90 !important;
            backdrop-filter: blur(10px) !important;
        }
        
        /* Status text styling */
        .status-text {
            font-family: 'JetBrains Mono', monospace;
            font-size: 0.9rem;
            color: rgba(64, 224, 208, 0.8);
            text-align: center;
            margin: 1rem 0;
            letter-spacing: 0.5px;
        }
        
        /* Grid layout for action buttons */
        .action-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 1.5rem;
            margin-top: 2rem;
        }
        
        /* Descriptive text styling */
        .description-text {
            font-family: 'Space Grotesk', sans-serif;
            font-size: 1.1rem;
            color: rgba(255, 255, 255, 0.7);
            line-height: 1.7;
            margin-bottom: 2rem;
            text-align: center;
        }
        
        /* Data visualization preview */
        .viz-preview {
            background: linear-gradient(135deg, 
                rgba(64, 224, 208, 0.05), 
                rgba(135, 206, 235, 0.05));
            border: 1px solid rgba(64, 224, 208, 0.2);
            border-radius: 16px;
            padding: 2rem;
            margin: 2rem 0;
            text-align: center;
        }
        
        /* Responsive design */
        @media (max-width: 768px) {
            .company-name {
                font-size: 2.5rem;
            }
            .brand-section {
                flex-direction: column;
                text-align: center;
            }
            .logo-placeholder {
                width: 60px;
                height: 60px;
                font-size: 2rem;
            }
            .holo-card {
                padding: 1.5rem;
                margin: 1rem 0;
            }
            .action-grid {
                grid-template-columns: 1fr;
                gap: 1rem;
            }
        }
        
        /* Hide Streamlit elements */
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        header {visibility: hidden;}
        .stDeployButton {display: none;}
    </style>
""", unsafe_allow_html=True)

# --- Main Content Container ---
st.markdown('<div class="main-content">', unsafe_allow_html=True)

# --- Futuristic Header with Logo ---
header_col1, header_col2 = st.columns([1, 5])

with header_col1:
    # Logo section - replace the path with your actual logo file
    try:

        #BASE_DIR = os.path.dirname(__file__)
        logo_path = os.path.join(BASE_DIR, "logo", "herbal-logo.avif")
        st.image(logo_path, width=200)

    except:
        st.markdown("""
            <div style="width: 80px; height: 80px; background: linear-gradient(135deg, #40e0d0, #87ceeb); 
                        border-radius: 20px; display: flex; align-items: center; justify-content: center; 
                        font-size: 2.5rem; box-shadow: 0 8px 32px rgba(64, 224, 208, 0.3);">
                🌿
            </div>
        """, unsafe_allow_html=True)

with header_col2:
    st.markdown("""
        <div style="padding-left: 2rem;">
            <h1 class="company-name">HERBAL GOODNESS</h1>
            <p class="tagline">// INVENTORY INTELLIGENCE SYSTEM //</p>
        </div>
    """, unsafe_allow_html=True)

# --- Session state setup ---
if "excel_buffer" not in st.session_state:
    st.session_state.excel_buffer = None
    st.session_state.filename = None
    st.session_state.drive_file_id = None
    st.session_state.file_downloaded = False

# --- AI Forecast Engine Section ---
st.markdown("""
    <div class="holo-card">
        <h2 class="section-title">🔮 ENHANCED FORECAST ENGINE</h2>
        <p class="description-text">
            Unleash the power of quantum analytics and machine learning algorithms 
            to predict inventory patterns with unprecedented accuracy. Our neural networks 
            analyze multidimensional data streams in real-time.
        </p>
    </div>
""", unsafe_allow_html=True)

# Create containers for dynamic content
button_container = st.container()
progress_container = st.container()
result_container = st.container()

with button_container:
    if st.button("🚀 INITIATE FORECASTING ANALYSIS", key="generate_btn"):

        start_time = time.time()  # Track when button was clicked

        with progress_container:
            with st.spinner("⚡ Generating Forecast Analysis..."):
                progress_bar = st.progress(0)
                status_text = st.empty()

                neural_stages = [
                    "⚡ Initializing Data Collection...",
                    "🧠 Sourcing Google Sheets Data...",
                    "📊 Mapping the SKUs...",
                    "🔍 Moving to The Forecast Algorithms...",
                    "⚙️ Optimizing Prediction Algorithms...",
                    "🌐 Synchronizing Multi-Channel Data...",
                    "✨ Generating Intelligence Reports...",
                ]

                # Holder for main() results
                results = {}

                # Start main() in a separate thread
                thread = threading.Thread(
                    target=lambda: results.update(
                        zip(("excel_buffer", "filename", "drive_file_id"), main())
                    )
                )
                thread.start()

                # Progress simulation while main() runs
                for i in range(100):
                    stage_index = i // 14
                    if stage_index < len(neural_stages):
                        status_text.markdown(
                            f'<p class="status-text">{neural_stages[stage_index]}</p>',
                            unsafe_allow_html=True
                        )
                    time.sleep(0.5)
                    progress_bar.progress(i + 1)

                    # If main() finishes early, break out
                    if not thread.is_alive():
                        break

                # Ensure main() has finished
                thread.join()

                # Clear progress UI
                progress_bar.empty()
                status_text.empty()

                # Show results
                if results.get("excel_buffer"):
                    st.session_state.excel_buffer = results["excel_buffer"]
                    st.session_state.filename = results["filename"]
                    st.session_state.drive_file_id = results["drive_file_id"]

                    end_time = time.time()
                    duration_sec = end_time - start_time
                    duration_str = time.strftime("%M:%S", time.gmtime(duration_sec))
                    timestamp_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                    with result_container:
                        st.success(
                            f"✅ FORECAST ANALYSIS COMPLETED | REPORTS READY!\n\n"
                            f"📅 Generated at: **{timestamp_str}**\n"
                            f"⏱ Duration taken for analysis: **{duration_str}**")
                else:
                    with result_container:
                        st.error("❌ Forecast Analysis Failed. Please try again.")

# --- Neural Access Portal ---
if st.session_state.excel_buffer:
    st.markdown("""
        <div class="holo-card">
            <h2 class="section-title">🌐 INTELLIGENCE ACCESS PORTAL</h2>
            <p class="description-text">
                Your enhanced forecast is now available across multiple dimensional platforms. 
                Access your intelligence reports through various interfaces for maximum operational efficiency.
            </p>
        </div>
    """, unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns(3, gap="large")

    with col1:
        if st.download_button(
            label="📥 DOWNLOAD EXCEL WORKBOOK" if not st.session_state.file_downloaded else "✅ WORKBOOK DOWNLOADED",
            data=st.session_state.excel_buffer.getvalue(),
            file_name=st.session_state.filename,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            on_click=lambda: st.session_state.update({"file_downloaded": True}),
            use_container_width=True,
            help="Download forecast intelligence in Excel format"
        ):
            pass

    with col2:
        if st.session_state.drive_file_id:
            file_id = st.session_state.drive_file_id
            sheets_link = f"https://docs.google.com/spreadsheets/d/{file_id}/edit"
            st.markdown(f"""
                <a href="{sheets_link}" target="_blank" class="neural-btn">
                    ☁️ GOOGLE SHEET VIEW
                </a>
            """, unsafe_allow_html=True)

    with col3:
        looker_url = "https://lookerstudio.google.com/reporting/9525ae1e-6f0e-4b5f-ae50-ca84312b76fd/page/br5SF"
        st.markdown(f"""
            <a href="{looker_url}" target="_blank" class="neural-btn">
                🗠 LOOKER DASHBOARD
            </a>
        """, unsafe_allow_html=True)

# --- Data Visualization Preview ---
st.markdown("""
    <div class="viz-preview">
        <h3 style="color: #40e0d0; font-family: 'Space Grotesk', sans-serif; margin-bottom: 1rem;">
            📈 PREDICTIVE VISUALIZATION MATRIX
        </h3>
        <p style="color: rgba(255, 255, 255, 0.6); font-family: 'JetBrains Mono', monospace; font-size: 0.9rem;">
            Real-time quantum analytics • Multi-dimensional forecasting • Neural pattern recognition
        </p>
    </div>
""", unsafe_allow_html=True)

# --- Footer ---
st.markdown("""
    <div style="text-align: center; padding: 3rem 0 2rem 0; margin-top: 3rem; border-top: 1px solid rgba(64, 224, 208, 0.2);">
        <p style="color: rgba(64, 224, 208, 0.8); font-family: 'JetBrains Mono', monospace; font-size: 0.9rem; margin: 0;">
            HERBAL GOODNESS © 2025 | POWERED BY AMW CONSULTANCY | VERSION 4.0.1
        </p>
        <p style="color: rgba(255, 255, 255, 0.4); font-family: 'Space Grotesk', sans-serif; font-size: 0.8rem; margin: 0.5rem 0 0 0;">
            Revolutionizing inventory management through intelligent forecasting
        </p>
    </div>
""", unsafe_allow_html=True)

st.markdown('</div>', unsafe_allow_html=True)