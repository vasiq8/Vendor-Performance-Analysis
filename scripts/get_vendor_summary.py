import sqlite3
import pandas as pd
import logging
from ingestion.db import ingest_db

logging.basicConfig(
    filename = "logs/get_vendor_summary.log",
    level = logging.DEBUG,
    format = "%(asctime)s - %(levelname)s - %(message)s",
    filemode = "a "
)

def create_vendor_summary(conn):
    vendor_sales_summary = pd.read_sql_query("""
    WITH FreightSummary AS(
    SELECT
        VendorNumber,
        SUM(Freight) AS FreightCost
    FROM vendor_invoice
    GROUP BY VendorNumber
    ),

    PurchaseSummary AS (
    SELECT 
        p.VendorNumber,
        p.VendorName, 
        p.Brand,
        p.Description,  
        p.PurchasePrice,
        pp.Price AS ActualPrice,
        pp.Volume,
        SUM(p.Quantity) AS TotalPurchaseQuantity,
        SUM(p.Dollars) AS TotalPurchaseDollars
    FROM purchases p
    JOIN purchase_prices pp
        ON p.Brand = pp.Brand
    WHERE p.PurchasePrice > 0
    GROUP BY p.VendorNumber, p.VendorName, p.Brand, p.Description, p.PurchasePrice, pp.Price, pp.Volume
    ),

    SalesSummary AS (
    SELECT 
        VendorNo,
        Brand,
        SUM(SalesQuantity) AS TotalSalesQuantity,
        SUM(SalesDollars) AS TotalSalesDollars,
        SUM(SalesPrice) AS TotalSalesPrice,
        SUM(ExciseTax) AS TotalExciseTax 
    FROM sales
    GROUP BY VendorNo, Brand
    )

    SELECT 
    ps.VendorNumber,
    ps.VendorName,
    ps.Brand,
    ps.Description,
    ps.PurchasePrice,
    ps.ActualPrice,
    ps.Volume,
    ps.TotalPurchaseQuantity,
    ps.TotalPurchaseDollars,
    ss.TotalSalesQuantity,
    ss.TotalSalesDollars,
    ss.TotalSalesPrice,
    ss.TotalExciseTax,
    fs.FreightCost
    FROM PurchaseSummary ps
    LEFT JOIN SalesSummary ss
        ON ps.VendorNumber = ss.VendorNo
        AND ps.Brand = ss.Brand
    LEFT JOIN FreightSummary fs
        ON ps.VendorNumber = fs.VendorNumber
    ORDER BY ps.TotalPurchaseDollars DESC
    """, conn)

    return vendor_sales_summary 


def clean_data(df):
    vendor_sales_summary['Volume'] = vendor_sales_summary['Volume'].astype('float64')
    vendor_sales_summary.fillna(0,inplace = True)
    vendor_sales_summary['VendorName'] = vendor_sales_summary['VendorName'].str.strip()
    
    vendor_sales_summary['GrossProfit'] = vendor_sales_summary['TotalSalesDollars'] - vendor_sales_summary['TotalPurchaseDollars']
    vendor_sales_summary['ProfitMargin'] = (
        vendor_sales_summary['GrossProfit'] / 
        vendor_sales_summary['TotalSalesDollars'].replace(0, np.nan)
    ) * 100
    vendor_sales_summary['StockTurnover'] = (
        vendor_sales_summary['TotalSalesQuantity'] / 
        vendor_sales_summary['TotalPurchaseQuantity'].replace(0, np.nan)
    )
    vendor_sales_summary['SalesToPurchaseRatio'] = (
        vendor_sales_summary['TotalSalesDollars'] / 
        vendor_sales_summary['TotalPurchaseDollars'].replace(0, np.nan)
    return df

