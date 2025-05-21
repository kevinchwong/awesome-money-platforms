import os
import sys
import json
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime
import logging

# Configuration
BATCH_SIZE = 500  # Maximum number of operations per batch

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def validate_environment_vars(required_vars):
    """Validate that all required environment variables are set."""
    missing_vars = [var for var in required_vars if not os.environ.get(var)]
    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        sys.exit(1)

def get_collection_name():
    """Get collection name from environment variable."""
    return os.environ.get('FIREBASE_PLATFORM_COLLECTION')

def initialize_firebase():
    """Initialize Firebase with proper error handling."""
    try:
        # Validate required environment variables
        validate_environment_vars(['GCP_SA_KEY', 'FIREBASE_PROJECT_ID', 'FIREBASE_PLATFORM_COLLECTION'])
        
        # Retrieve and parse the GCP service account key
        gcp_sa_key = os.environ.get('GCP_SA_KEY')
        if not gcp_sa_key:
            raise ValueError("GCP_SA_KEY environment variable is not set or is empty.")
        service_account_info = json.loads(gcp_sa_key)
        
        # Log the service account info (without sensitive data)
        logger.info(f"Initializing Firebase with project ID: {service_account_info.get('project_id', 'N/A')}")
        logger.info(f"Using service account email: {service_account_info.get('client_email', 'N/A')}")
        
        # Initialize Firebase
        cred = credentials.Certificate(service_account_info)
        firebase_admin.initialize_app(cred)
        return firestore.client()
    except json.JSONDecodeError:
        logger.error("GCP_SA_KEY is not a valid JSON string. Please check the format of your service account key.")
        sys.exit(1)
    except ValueError as ve:
        logger.error(str(ve))
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error initializing Firebase: {str(e)}")
        logger.error("Please check your GCP_SA_KEY and other environment variables.")
        sys.exit(1)

def create_document(db, doc_id=None):
    """Create a new document with optional custom ID."""
    try:
        collection_ref = db.collection(get_collection_name())
        doc_ref = collection_ref.document(doc_id) if doc_id else collection_ref.document()
        
        data = {
            'name': 'Example Platform',
            'description': 'A sample platform entry',
            'created_at': firestore.SERVER_TIMESTAMP,
            'updated_at': firestore.SERVER_TIMESTAMP,
            'status': 'active',
            'type': 'platform',
            'metadata': {
                'version': '1.0',
                'tags': ['example', 'platform']
            }
        }
        
        doc_ref.set(data)
        logger.info(f"Document created successfully with ID: {doc_ref.id}")
        return doc_ref.id
    except Exception as e:
        logger.error(f"Error creating document: {str(e)}")
        sys.exit(1)

def clean_collection(db):
    """Delete all documents in the collection using batch operations."""
    try:
        collection_ref = db.collection(get_collection_name())
        docs = collection_ref.stream()
        
        # Use batch operations for better performance
        batch = db.batch()
        count = 0
        batch_count = 0
        
        for doc in docs:
            batch.delete(doc.reference)
            count += 1
            batch_count += 1
            
            # Commit batch when it reaches the size limit
            if batch_count >= BATCH_SIZE:
                batch.commit()
                logger.info(f"Committed batch of {batch_count} deletions")
                batch = db.batch()
                batch_count = 0
        
        # Commit any remaining operations
        if batch_count > 0:
            batch.commit()
            logger.info(f"Committed final batch of {batch_count} deletions")
        
        logger.info(f"Successfully deleted {count} documents from {get_collection_name()}")
    except Exception as e:
        logger.error(f"Error cleaning collection: {str(e)}")
        sys.exit(1)
    
def get_documents(db, doc_id=None):
    """Get documents with optional filtering by ID."""
    try:
        collection_ref = db.collection(get_collection_name())
        
        if doc_id:
            doc = collection_ref.document(doc_id).get()
            if not doc.exists:
                logger.warning(f"No document found with ID: {doc_id}")
                return
            docs = [doc]
        else:
            docs = collection_ref.stream()
        
        count = 0
        logger.info(f"\nDocuments in {get_collection_name()}:")
        print("-" * 50)
        
        for doc in docs:
            data = doc.to_dict()
            print(f"Document ID: {doc.id}")
            for key, value in data.items():
                if isinstance(value, dict):
                    print(f"{key}:")
                    for k, v in value.items():
                        print(f"  {k}: {v}")
                else:
                    print(f"{key}: {value}")
            print("-" * 50)
            count += 1
        
        logger.info(f"Total documents: {count}")
    except Exception as e:
        logger.error(f"Error retrieving documents: {str(e)}")
        sys.exit(1)
        
def update_document(db, doc_id=None):
    """Update a document with optional ID specification."""
    try:
        collection_ref = db.collection(get_collection_name())
        
        if doc_id:
            doc_ref = collection_ref.document(doc_id)
            if not doc_ref.get().exists:
                logger.warning(f"No document found with ID: {doc_id}")
                return
        else:
            docs = list(collection_ref.limit(1).stream())
            if not docs:
                logger.warning("No documents found to update")
                return
            doc_ref = docs[0].reference
        
        update_data = {
            'name': 'Updated Platform',
            'updated_at': firestore.SERVER_TIMESTAMP,
            'status': 'updated',
            'metadata.version': '1.1',
            'metadata.last_updated': datetime.now().isoformat()
        }
        
        doc_ref.update(update_data)
        logger.info(f"Document {doc_ref.id} updated successfully")
    except Exception as e:
        logger.error(f"Error updating document: {str(e)}")
        sys.exit(1)
    
def delete_document(db, doc_id=None):
    """Delete a document with optional ID specification."""
    try:
        collection_ref = db.collection(get_collection_name())
        
        if doc_id:
            doc_ref = collection_ref.document(doc_id)
            if not doc_ref.get().exists:
                logger.warning(f"No document found with ID: {doc_id}")
                return
        else:
            docs = list(collection_ref.limit(1).stream())
            if not docs:
                logger.warning("No documents found to delete")
                return
            doc_ref = docs[0].reference
        
        doc_ref.delete()
        logger.info(f"Document {doc_ref.id} deleted successfully")
    except Exception as e:
        logger.error(f"Error deleting document: {str(e)}")
        sys.exit(1)

def main():
    if len(sys.argv) < 2:
        print("Usage: python firebase_operations.py [create|clean|get|update|delete] [optional_doc_id]")
        sys.exit(1)

    operation = sys.argv[1]
    doc_id = sys.argv[2] if len(sys.argv) > 2 else None
    
    db = initialize_firebase()

    operations = {
        'create': create_document,
        'clean': clean_collection,
        'get': get_documents,
        'update': update_document,
        'delete': delete_document
    }

    if operation in operations:
        logger.info(f"Executing {operation} operation on {get_collection_name()} collection...")
        if operation in ['create', 'get', 'update', 'delete']:
            operations[operation](db, doc_id)
        else:
            operations[operation](db)
    else:
        logger.error(f"Invalid operation. Use one of: {', '.join(operations.keys())}")
        sys.exit(1)

if __name__ == "__main__":
    main() 