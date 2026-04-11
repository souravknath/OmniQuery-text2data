
# MongoDB CustomerDB Metadata
MONGODB_CUSTOMER_DB_SCHEMA = {
    "customers": {
        "description": "User profile information, segments, and financial metrics.",
        "fields": {
            "customer_id": "Unique UUID for the customer",
            "profile": {
                "name": "Full name",
                "age": "Age in years",
                "gender": "Male or Female"
            },
            "location_id": "INT - links to SQL Locations.dbo.Locations.LocationId",
            "segments": "LIST - labels like 'Premium', 'New', 'Frequent Buyer', 'Churn Risk'",
            "financial": {
                "total_spent": "Total amount spent by customer",
                "avg_order_value": "Average value per order"
            },
            "is_active": "Boolean status of account"
        }
    },
    "activities": {
        "description": "Log of customer interactions and purchases.",
        "fields": {
            "activity_id": "Unique UUID",
            "customer_id": "Reference to customers.customer_id",
            "activity_type": "View, Cart, or Purchase",
            "product_category": "Electronics, Fashion, or Grocery",
            "amount": "Transaction amount (if purchase)",
            "timestamp": "ISO Date of activity"
        }
    },
    "support_tickets": {
        "description": "Customer support interaction history.",
        "fields": {
            "ticket_id": "Unique UUID",
            "customer_id": "Reference to customers.customer_id",
            "issue_type": "Payment, Delivery, or Refund",
            "status": "Open or Closed",
            "priority": "Low, Medium, or High",
            "created_at": "ISO Date"
        }
    }
}

MONGODB_CUSTOMER_DB_SAMPLES = {
    "customers": [
        {
            "customer_id": "550e8400-e29b-41d4-a716-446655440000",
            "profile": {"name": "Arjun Sharma", "age": 28, "gender": "Male"},
            "location_id": 5,
            "segments": ["Premium", "Frequent Buyer"],
            "financial": {"total_spent": 15400, "avg_order_value": 2200},
            "is_active": True
        }
    ],
    "activities": [
        {
            "customer_id": "550e8400-e29b-41d4-a716-446655440000",
            "activity_type": "Purchase",
            "product_category": "Electronics",
            "amount": 12000,
            "timestamp": "2024-03-20T14:30:00Z"
        }
    ]
}

# SQL Server Users & Orders Metadata
SQL_USERS_ORDERS_DB_SCHEMA = {
    "Users": {
        "columns": ["UserId (UUID)", "FirstName (NVARCHAR)", "LastName (NVARCHAR)", "EmailId (NVARCHAR)", "UserName (NVARCHAR)", "LocationId (INT)"],
        "relationships": "LocationId links to Location.dbo.Locations.LocationId"
    },
    "Orders": {
        "columns": ["OrderId (UUID)", "OrderName (NVARCHAR)", "Amount (DECIMAL)", "OrderDate (DATETIME)"]
    },
    "User_Orders": {
        "columns": ["Id (INT)", "UserId (UUID)", "OrderId (UUID)"],
        "relationships": "Joins Users and Orders tables"
    }
}

SQL_USERS_ORDERS_DB_SAMPLES = {
    "Users": [
        {"UserId": "A1B2C3D4...", "FirstName": "Sita", "LastName": "Rao", "EmailId": "sita.rao@example.com", "UserName": "sita.rao_88", "LocationId": 12}
    ],
    "Orders": [
        {"OrderId": "B2C3D4E5...", "OrderName": "Laptop - Zenbook", "Amount": 85000.50, "OrderDate": "2024-04-01 10:15:00"}
    ]
}

# SQL Server Locations Metadata
SQL_LOCATIONS_DB_SCHEMA = {
    "Locations": {
        "columns": ["LocationId (INT)", "Address (NVARCHAR)", "City (NVARCHAR)", "State (NVARCHAR)", "Country (NVARCHAR)", "ZipCode (NVARCHAR)"]
    }
}

SQL_LOCATIONS_DB_SAMPLES = {
    "Locations": [
        {"LocationId": 5, "Address": "123 MG Road", "City": "Bangalore", "State": "Karnataka", "Country": "India", "ZipCode": "560001"}
    ]
}
