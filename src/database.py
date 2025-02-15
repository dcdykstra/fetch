import duckdb


def load_raw_model1_tables():
    conn = duckdb.connect("fetch.db")
    load_raw(conn)
    build_model1_schema(conn)
    load_model1_users(conn)
    load_model1_brands(conn)
    load_model1_receipts(conn)
    load_model1_rewards_receipts_items(conn)
    conn.close()


def drop_tables(conn):
    conn.execute(
        """
        DROP TABLE users;
        DROP TABLE brands;
        DROP TABLE receipts;
        DROP TABLE receipt_items;
        DROP TABLE users_raw;
        DROP TABLE brands_raw;
        DROP TABLE receipts_raw
        """
    )


def load_raw(conn):
    conn.execute(
        """
    CREATE TABLE IF NOT EXISTS users_raw AS 
        SELECT * FROM read_json_auto('data/users.json');  
                
    CREATE TABLE IF NOT EXISTS brands_raw AS 
        SELECT * FROM read_json_auto('data/brands.json');
                
    CREATE TABLE IF NOT EXISTS receipts_raw AS 
        SELECT * FROM read_json_auto('data/receipts.json');
                """
    )


def build_model1_schema(conn):
    conn.execute(
        """
    CREATE TABLE IF NOT EXISTS users (
        _user_id VARCHAR PRIMARY KEY,
        state VARCHAR,
        created_date TIMESTAMP,
        last_login TIMESTAMP,
        role VARCHAR,
        active BOOLEAN,
        sign_up_source VARCHAR
    );
                
    CREATE TABLE IF NOT EXISTS brands (
        _brand_id VARCHAR PRIMARY KEY,
        barcode VARCHAR,
        brand_name VARCHAR,
        brand_code VARCHAR,
        category VARCHAR,
        category_code VARCHAR,
        _cpg_id VARCHAR,
        cpg_ref VARCHAR,
        top_brand BOOLEAN
    );

    CREATE TABLE IF NOT EXISTS receipts (
        _receipt_id VARCHAR PRIMARY KEY,
        _user_id VARCHAR,
        bonus_points_earned INTEGER,
        bonus_points_earned_reason VARCHAR,
        create_date TIMESTAMP,
        date_scanned TIMESTAMP,
        finished_date TIMESTAMP,
        modify_date TIMESTAMP,
        points_awarded_date TIMESTAMP,
        points_earned INTEGER,
        purchase_date TIMESTAMP,
        purchased_item_count INTEGER,
        rewards_receipt_status VARCHAR,
        total_spent DECIMAL(10,2)
    );

    CREATE TABLE IF NOT EXISTS receipt_items (
        _receipt_item_id VARCHAR PRIMARY KEY,
        _receipt_id VARCHAR,
        _user_id VARCHAR,
        _metabrite_campaign_id VARCHAR,
        _partner_item_id VARCHAR,
        _points_payer_id VARCHAR,
        _rewards_product_partner_id VARCHAR,
        barcode VARCHAR,
        brand_code VARCHAR,
        competitive_product VARCHAR,
        competitor_rewards_group VARCHAR,
        deleted VARCHAR,
        description VARCHAR,
        discounted_item_price VARCHAR,
        final_price VARCHAR,
        item_number VARCHAR,
        item_price VARCHAR,
        needs_fetch_review VARCHAR,
        needs_fetch_review_reason VARCHAR,
        original_final_price VARCHAR,
        original_metabrite_barcode VARCHAR,
        original_metabrite_description VARCHAR,
        original_metabrite_item_price VARCHAR,
        original_metabrite_quantity_purchased VARCHAR,
        original_receipt_item_text VARCHAR,
        points_earned VARCHAR,
        points_not_awarded_reason VARCHAR,
        prevent_target_gap_points VARCHAR,
        price_after_coupon VARCHAR,
        quantity_purchased VARCHAR,
        rewards_group VARCHAR,
        target_price VARCHAR,
        user_flagged_barcode VARCHAR,
        user_flagged_description VARCHAR,
        user_flagged_new_item VARCHAR,
        user_flagged_price VARCHAR,
        user_flagged_quantity VARCHAR
    );
    """
    )


def load_model1_users(conn):
    conn.execute(
        """
    WITH distinct_users AS (
        SELECT DISTINCT * FROM users_raw
    )
    INSERT INTO users
    SELECT
        _id::JSON->>'$.$oid' as _user_id, 
        state,
        TO_TIMESTAMP((createdDate::JSON->>'$.$date')::numeric/1000) as created_date,
        TO_TIMESTAMP((lastLogin::JSON->>'$.$date')::numeric/1000) as last_login,
        role,
        active,
        signUpSource as sign_up_source
    FROM distinct_users;
    """
    )


def load_model1_brands(conn):
    conn.execute(
        """
    WITH distinct_brands AS (
        SELECT DISTINCT * FROM brands_raw
    )
    INSERT INTO brands
    SELECT
        _id::JSON->>'$.$oid' as _brand_id, 
        barcode,
        name as brand_name,
        brandCode as brand_code,
        category,
        categoryCode as category_code,
        cpg::JSON->'$.$id'->>'$.$oid' as _cpg_id,
        cpg::JSON->>'$.$ref' as cpg_ref,
        topBrand
    FROM distinct_brands;
    """
    )


def load_model1_receipts(conn):
    conn.execute(
        """
    WITH distinct_receipts AS (
        SELECT DISTINCT * FROM receipts_raw
    )

    INSERT INTO receipts
    SELECT
        _id::JSON->>'$.$oid' as _receipt_id,
        userId as _user_id,
        bonusPointsEarned as bonus_points_earned,
        bonusPointsEarnedReason as bonus_points_earned_reason,
        TO_TIMESTAMP((createDate::JSON->>'$.$date')::numeric/1000) as create_date,
        TO_TIMESTAMP((dateScanned::JSON->>'$.$date')::numeric/1000) as date_scanned,
        TO_TIMESTAMP((finishedDate::JSON->>'$.$date')::numeric/1000) as finished_date,
        TO_TIMESTAMP((modifyDate::JSON->>'$.$date')::numeric/1000) as modify_date,
        TO_TIMESTAMP((pointsAwardedDate::JSON->>'$.$date')::numeric/1000) as points_awarded_date,
        pointsEarned as points_earned,
        TO_TIMESTAMP((purchaseDate::JSON->>'$.$date')::numeric/1000) as purchase_date,
        purchasedItemCount as purchase_item_count,
        rewardsReceiptStatus as rewards_receipt_status,
        totalSpent as total_spent
    FROM distinct_receipts;
    """
    )


def load_model1_rewards_receipts_items(conn):
    conn.execute(
        """
    WITH expanded_rewards_receipt_item_list AS (
        SELECT
        _id::JSON->>'$.$oid' as _receipt_id,
        userId as _user_id,
        UNNEST(rewardsReceiptItemList) as rewardsReceiptItemListExpanded
        FROM (SELECT DISTINCT * FROM receipts_raw)
    )

    INSERT INTO receipt_items
    SELECT 
        row_number() OVER (ORDER BY _receipt_id) as _receipt_item_id,
        _receipt_id,
        _user_id,
        rewardsReceiptItemListExpanded::JSON->>'$.barcode' as barcode,
        rewardsReceiptItemListExpanded::JSON->>'$.brandCode' as brand_code,
        rewardsReceiptItemListExpanded::JSON->>'$.competiveProduct' as competitive_product,
        rewardsReceiptItemListExpanded::JSON->>'$.competitorRewardsGroup' as competitor_rewards_group,
        rewardsReceiptItemListExpanded::JSON->>'$.deleted' as deleted,
        rewardsReceiptItemListExpanded::JSON->>'$.description' as description,
        rewardsReceiptItemListExpanded::JSON->>'$.discountedItemPrice' as discounted_item_price,
        rewardsReceiptItemListExpanded::JSON->>'$.finalPrice' as final_price,
        rewardsReceiptItemListExpanded::JSON->>'$.itemNumber' as item_number,
        rewardsReceiptItemListExpanded::JSON->>'$.itemPrice' as item_price,
        rewardsReceiptItemListExpanded::JSON->>'$.metabriteCampaignId' as _metabrite_campaign_id,
        rewardsReceiptItemListExpanded::JSON->>'$.needsFetchReview' as needs_fetch_review,
        rewardsReceiptItemListExpanded::JSON->>'$.needsFetchReviewReason' as needs_fetch_review_reason,
        rewardsReceiptItemListExpanded::JSON->>'$.originalFinalPrice' as final_price,
        rewardsReceiptItemListExpanded::JSON->>'$.originalMetaBriteBarcode' as metabrite_barcode,
        rewardsReceiptItemListExpanded::JSON->>'$.originalMetaBriteDescription' as metabrite_description,
        rewardsReceiptItemListExpanded::JSON->>'$.originalMetaBriteItemPrice' as metabrite_item_price,
        rewardsReceiptItemListExpanded::JSON->>'$.originalMetaBriteQuantityPurchased' as metabrite_quantity_purchased,
        rewardsReceiptItemListExpanded::JSON->>'$.originalReceiptItemText' as original_recipt_item_text,
        rewardsReceiptItemListExpanded::JSON->>'$.partnerItemId' as _partner_item_id,
        rewardsReceiptItemListExpanded::JSON->>'$.pointsEarned' as points_earned,
        rewardsReceiptItemListExpanded::JSON->>'$.pointsNotAwardedReason' as points_not_awarded_reason,
        rewardsReceiptItemListExpanded::JSON->>'$.pointsPayerId' as _points_payer_id,
        rewardsReceiptItemListExpanded::JSON->>'$.preventTargetGapPoints' as prevent_target_gap_points,
        rewardsReceiptItemListExpanded::JSON->>'$.priceAfterCoupon' as price_after_coupon,
        rewardsReceiptItemListExpanded::JSON->>'$.quantityPurchased' as quantity_purchased,
        rewardsReceiptItemListExpanded::JSON->>'$.rewardsGroup' as rewards_group,
        rewardsReceiptItemListExpanded::JSON->>'$.rewardsProductPartnerId' as _rewards_product_partner_id,
        rewardsReceiptItemListExpanded::JSON->>'$.targetPrice' as target_price,
        rewardsReceiptItemListExpanded::JSON->>'$.userFlaggedBarcode' as user_flagged_barcode,
        rewardsReceiptItemListExpanded::JSON->>'$.userFlaggedDescription' as user_flagged_description,
        rewardsReceiptItemListExpanded::JSON->>'$.userFlaggedNewItem' as user_flagged_new_item,
        rewardsReceiptItemListExpanded::JSON->>'$.userFlaggedPrice' as user_flagged_price,
        rewardsReceiptItemListExpanded::JSON->>'$.userFlaggedQuantity' as user_flagged_quantity,
    FROM expanded_rewards_receipt_item_list;
    """
    )
