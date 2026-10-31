# Dataset Setup & Instructions

This project uses the **Brazilian E-Commerce Public Dataset by Olist** released on Kaggle.

- **Kaggle Source:** [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
- **License:** CC BY-NC-SA 4.0

---

## 1. Directory Structure

```text
data/
├── sample/               # Synthetic, tiny sample CSV files (committed to git for instant testing)
│   ├── olist_orders_dataset.csv
│   ├── olist_order_items_dataset.csv
│   ├── olist_order_payments_dataset.csv
│   ├── olist_customers_dataset.csv
│   ├── olist_products_dataset.csv
│   ├── olist_sellers_dataset.csv
│   ├── olist_order_reviews_dataset.csv
│   └── product_category_name_translation.csv
└── raw/                  # Full Kaggle CSVs (ignored by git, put downloaded files here)
```

---

## 2. Downloading Full Kaggle Data

### Option A: Via Browser
1. Visit [Kaggle Dataset Page](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).
2. Download the archive zip file.
3. Unzip all CSV files into the `data/raw/` directory.

### Option B: Via Kaggle CLI
If you have the `kaggle` CLI configured:
```bash
kaggle datasets download -d olistbr/brazilian-ecommerce -p data/raw/ --unzip
```

---

## 3. Using Sample Data for Quick Local Testing
For rapid development, CI, and code validation, the `data/sample/` folder contains synthetic sample records matching the exact schema and data types of the full dataset. You can test the entire pipeline locally without downloading the 100k+ row Kaggle dataset.

