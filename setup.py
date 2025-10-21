import os
from database.connection import DatabaseConnection
from database.queries import QueryService


def create_audit_log_table():
    """Create the audit_log table if it doesn't exist"""
    print("Creating audit_log table...")
    
    query = """
    CREATE EXTERNAL TABLE IF NOT EXISTS audit_log (
        id STRING,
        table_name STRING,
        operation STRING,
        record_id STRING,
        field_name STRING,
        old_value STRING,
        new_value STRING,
        changed_by STRING,
        changed_at TIMESTAMP,
        notes STRING
    )
    STORED AS PARQUET
    LOCATION 's3://{bucket}/audit_log/'
    TBLPROPERTIES ('has_encrypted_data'='false')
    """.format(bucket=os.getenv('S3_BUCKET', 'your-bucket'))
    
    try:
        DatabaseConnection.execute_query(query)
        print("✅ Audit log table created successfully")
        return True
    except Exception as e:
        print(f"❌ Error creating audit log table: {e}")
        return False


def verify_tables():
    """Verify that all required tables exist"""
    print("\nVerifying required tables...")
    
    required_tables = [
        'institution',
        'geography',
        'sector',
        'instrument',
        'institution_standardization'
    ]
    
    query_service = QueryService()
    
    for table in required_tables:
        try:
            result = DatabaseConnection.execute_query(f"SELECT COUNT(*) as count FROM {table} LIMIT 1")
            count = result.iloc[0]['count'] if not result.empty else 0
            print(f"✅ {table}: {count} rows")
        except Exception as e:
            print(f"❌ {table}: Error - {str(e)}")


def test_connection():
    """Test database connection"""
    print("Testing database connection...")
    
    try:
        query = "SELECT 1 as test"
        result = DatabaseConnection.execute_query(query)
        if not result.empty:
            print("✅ Database connection successful")
            return True
        else:
            print("❌ Database connection failed")
            return False
    except Exception as e:
        print(f"❌ Database connection error: {e}")
        return False


def check_environment():
    """Check that all required environment variables are set"""
    print("Checking environment configuration...")
    
    required_vars = [
        'AWS_REGION',
        'S3_BUCKET',
        'ATHENA_DATABASE',
        'ATHENA_OUTPUT_LOCATION'
    ]
    
    optional_vars = [
        'OPENAI_API_KEY'
    ]
    
    all_set = True
    
    for var in required_vars:
        value = os.getenv(var)
        if value:
            print(f"✅ {var}: {value[:20]}{'...' if len(value) > 20 else ''}")
        else:
            print(f"❌ {var}: Not set (REQUIRED)")
            all_set = False
    
    for var in optional_vars:
        value = os.getenv(var)
        if value:
            print(f"✅ {var}: Set (optional)")
        else:
            print(f"⚠️  {var}: Not set (optional - AI features disabled)")
    
    return all_set


def main():
    """Main setup function"""
    print("=" * 60)
    print("Reference Data Management System - Setup")
    print("=" * 60)
    print()
    
    # Step 1: Check environment
    print("STEP 1: Environment Configuration")
    print("-" * 60)
    env_ok = check_environment()
    print()
    
    if not env_ok:
        print("❌ Please set all required environment variables before continuing.")
        print("   See .env.example for reference.")
        return
    
    # Step 2: Test connection
    print("STEP 2: Database Connection")
    print("-" * 60)
    conn_ok = test_connection()
    print()
    
    if not conn_ok:
        print("❌ Database connection failed. Please check your AWS credentials and configuration.")
        return
    
    # Step 3: Create audit log table
    print("STEP 3: Create Audit Log Table")
    print("-" * 60)
    create_audit_log_table()
    print()
    
    # Step 4: Verify tables
    print("STEP 4: Verify Tables")
    print("-" * 60)
    verify_tables()
    print()
    
    # Complete
    print("=" * 60)
    print("✅ Setup complete!")
    print("=" * 60)
    print()
    print("You can now run the application with:")
    print("    streamlit run app.py")
    print()


if __name__ == "__main__":
    # Load environment variables from .env file
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        print("Warning: python-dotenv not installed. Using system environment variables only.")
    
    main()