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

def validate_environment():
    """Validate required environment variables."""
    required_vars = ['FIREBASE_TOKEN', 'FIREBASE_PROJECT_ID', 'FIREBASE_COLLECTION']
    missing_vars = [var for var in required_vars if not os.environ.get(var)]
    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        sys.exit(1)

def get_collection_name():
    """Get collection name from environment variable."""
    return os.environ.get('FIREBASE_COLLECTION')

def initialize_firebase():
    """Initialize Firebase with proper error handling."""
    try:
        validate_environment()
        
        # Create service account info from environment variables
        service_account_info = {
            "type": "service_account",
            "project_id": os.environ.get('FIREBASE_PROJECT_ID'),
            "private_key": os.environ.get('FIREBASE_TOKEN').replace('\\n', '\n'),
            "client_email": f"firebase-adminsdk-{os.environ.get('FIREBASE_PROJECT_ID')}@appspot.gserviceaccount.com",
            "client_id": "",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": f"https://www.googleapis.com/robot/v1/metadata/x509/firebase-adminsdk-{os.environ.get('FIREBASE_PROJECT_ID')}%40appspot.gserviceaccount.com"
        }
        
        # Log the service account info (without sensitive data)
        logger.info(f"Initializing Firebase with project ID: {service_account_info['project_id']}")
        logger.info(f"Using service account email: {service_account_info['client_email']}")
        
        # Initialize Firebase
        cred = credentials.Certificate(service_account_info)
        firebase_admin.initialize_app(cred)
        return firestore.client()
    except Exception as e:
        logger.error(f"Error initializing Firebase: {str(e)}")
        logger.error("Please check your FIREBASE_TOKEN and FIREBASE_PROJECT_ID values")
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