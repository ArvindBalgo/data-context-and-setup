import pandas as pd
import numpy as np
from olist.utils import haversine_distance
from olist.data import Olist


class Order:
    '''
    DataFrames containing all orders as index,
    and various properties of these orders as columns
    '''
    def __init__(self):
        # Assign an attribute ".data" to all new instances of Order
        self.data = Olist().get_data()

    def get_wait_time(self, is_delivered=True):
        """
        Returns a DataFrame with:
        [order_id, wait_time, expected_wait_time, delay_vs_expected, order_status]
        and filters out non-delivered orders unless specified
        """
        # Hint: Within this instance method, you have access to the instance of the class Order in the variable self, as well as all its attributes
        orders = self.data['orders'].copy()
        orders = orders[orders['order_status'] == 'delivered']
        date_columns = ['order_purchase_timestamp', 'order_approved_at', 'order_delivered_carrier_date', 'order_delivered_customer_date', 'order_estimated_delivery_date']

        for col in date_columns:
            orders[col] = pd.to_datetime(orders[col])

        orders["wait_time"] = orders['order_delivered_customer_date'] - orders['order_purchase_timestamp']
        orders["expected_wait_time"] = orders['order_estimated_delivery_date'] - orders['order_purchase_timestamp']
        orders["delay_vs_expected"] = (orders['order_delivered_customer_date'] - orders['order_estimated_delivery_date']).dt.days
        orders['delay_vs_expected'] = orders['delay_vs_expected'].apply(lambda x: x if x > 0 else 0)

        return orders[['order_id', 'wait_time', 'expected_wait_time', 'delay_vs_expected' ,'order_status']]


    def get_review_score(self):
        """
        Returns a DataFrame with:
        order_id, dim_is_five_star, dim_is_one_star, review_score
        """
        reviews = self.data['order_reviews'].copy()
        reviews['dim_is_five_star'] = reviews['review_score']
        reviews['dim_is_five_star'] = reviews['dim_is_five_star'].apply(lambda x: 1 if x == 5 else 0)
        reviews['dim_is_one_star'] = reviews['review_score']
        reviews['dim_is_one_star'] = reviews['dim_is_one_star'].apply(lambda x: 1 if x == 1 else 0)

        return reviews[['order_id', 'dim_is_five_star', 'dim_is_one_star', 'review_score']]



    def get_number_products(self):
        """
        Returns a DataFrame with:
        order_id, number_of_products
        """
        order_items = self.data['order_items'].copy()
        #order_items["number_of_products"] = order_items.groupby("order_id")["order_item_id"].transform("sum")
        order_product_sum = order_items.groupby('order_id')['order_item_id'].sum().reset_index()
        order_product_sum.rename(columns={'order_item_id': 'number_of_products'}, inplace=True)

        return order_product_sum

    def get_number_sellers(self):
        """
        Returns a DataFrame with:
        order_id, number_of_sellers
        """
        order_items = self.data['order_items'].copy()
        order_seller_count = order_items.groupby('order_id')['seller_id'].nunique().reset_index()
        order_seller_count.rename(columns={'seller_id': 'number_of_sellers'}, inplace=True)
        return order_seller_count


    def get_price_and_freight(self):
        """
        Returns a DataFrame with:
        order_id, price, freight_value
        """
        order_items = self.data['order_items'].copy()
        return order_items.groupby('order_id').agg({'price': 'sum', 'freight_value': 'sum'}).reset_index()

    # Optional
    def get_distance_seller_customer(self):
        """
        Returns a DataFrame with:
        order_id, distance_seller_customer
        """
        orders = self.data['orders']
        order_items = self.data['order_items']
        sellers = self.data['sellers']
        customers = self.data['customers']
        geolocation = self.data['geolocation']

        geolocation = geolocation.groupby('geolocation_zip_code_prefix', as_index=False).first()

        sellers_geo = sellers.merge(geolocation, how='left', left_on='seller_zip_code_prefix', right_on='geolocation_zip_code_prefix')
        sellers_geo = sellers_geo[['seller_id', 'seller_zip_code_prefix', 'geolocation_lat', 'geolocation_lng']]

        customers_geo = customers.merge(geolocation, how='left', left_on='customer_zip_code_prefix', right_on='geolocation_zip_code_prefix')
        customers_geo = customers_geo[['customer_id', 'customer_zip_code_prefix', 'geolocation_lat', 'geolocation_lng']]

        orders_transformed = orders[['order_id', 'customer_id']]

        order_items_transformed = order_items[['order_id', 'seller_id']]
        orders_customers_sellers = orders_transformed.merge(order_items_transformed, how='left', left_on='order_id', right_on='order_id')

        orders_customers_sellers_geo = orders_customers_sellers.merge(sellers_geo, on='seller_id')
        orders_customers_sellers_geo = orders_customers_sellers_geo.merge(customers_geo, on='customer_id', suffixes=('_seller','_customer'))
        orders_customers_sellers_geo = orders_customers_sellers_geo.dropna()
        orders_customers_sellers_geo['distance_seller_customer'] = orders_customers_sellers_geo.apply(lambda row: haversine_distance(row['geolocation_lng_seller'], row['geolocation_lat_seller'], row['geolocation_lng_customer'], row['geolocation_lat_customer']), axis=1)

        order_distance = orders_customers_sellers_geo.groupby('order_id', as_index=False).agg({'distance_seller_customer':'mean'})

        return order_distance

    def get_training_data(self,
                          is_delivered=True,
                          with_distance_seller_customer=False):
        """
        Returns a clean DataFrame (without NaN), with the all following columns:
        ['order_id', 'wait_time', 'expected_wait_time', 'delay_vs_expected',
        'order_status', 'dim_is_five_star', 'dim_is_one_star', 'review_score',
        'number_of_products', 'number_of_sellers', 'price', 'freight_value',
        'distance_seller_customer']
        """
        # Hint: make sure to re-use your instance methods defined above
        training_set =self.get_wait_time(is_delivered).merge(self.get_review_score(), on='order_id').merge(self.get_number_products(), on='order_id').merge(self.get_number_sellers(), on='order_id').merge(self.get_price_and_freight(), on='order_id')
        if with_distance_seller_customer:
            training_set = training_set.merge(self.get_distance_seller_customer(), on='order_id')

        return training_set.dropna()
