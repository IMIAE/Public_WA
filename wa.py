import json
import os
import WaApi
import urllib.parse
import pandas as pd

# Removed Excel

# Path to your configuration file
config_path = os.path.join(os.path.dirname(__file__), 'config.json')

# Load credentials from the configuration file
with open(config_path, 'r') as config_file:
    config = json.load(config_file)
    client_id = config['client_id']
    client_secret = config['client_secret']
    api_key = config['api_key']
    account_id = config['account_id']
# Initialize the client and authenticate
api = WaApi.WaApiClient(client_id, client_secret)
api.authenticate_with_apikey(api_key)

def get_contacts():
    request_url = 'https://api.wildapricot.org/v2.3/accounts/315251/contacts?idsOnly=true'
    return api.execute_request(request_url)

def get_contact_info(contact_id):
    request_url = f'https://api.wildapricot.org/v2.3/accounts/315251/contacts/{contact_id}'
    return api.execute_request(request_url)

# Function to fetch all sent emails with pagination
def get_all_emails():
    emails = []
    limit = 100
    offset = 0

    while True:
        params = {'$top': limit, '$skip': offset, 'idsOnly': 'true'}
        request_url = f'https://api.wildapricot.org/v2.3/accounts/{account_id}/SentEmails?' + urllib.parse.urlencode(params)
        response = api.execute_request(request_url)
        new_emails = getattr(response, 'EmailsIdentifiers', [])

        if not new_emails:
            break

        emails.extend(new_emails)
        offset += limit

    return emails

# Function to fetch email statistics for a specific email
def get_account_emails_stats(email_id):
    request_url = f'https://api.wildapricot.org/v2.3/accounts/{account_id}/SentEmailRecipients?emailId={email_id}&LoadLinks=true&Delivered=true&Opened=true'
    response = api.execute_request(request_url)
    return response

# Function to fetch event registrations for a specific event
def get_event_registrations(account_id, event_id):
    params = {
        'eventId': event_id,
        'includeDetails': 'true',
        'includeWaitlist': 'true'
    }
    request_url = f'https://api.wildapricot.org/v2.3/accounts/{account_id}/eventregistrations?' + urllib.parse.urlencode(params)
    print(f"Request URL: {request_url}")  # Debugging line to show the request URL
    response = api.execute_request(request_url)
    return response

# Function to fetch event registration details by registration ID
def get_event_registration_details(account_id, registration_id):
    request_url = f'https://api.wildapricot.org/v2.3/accounts/{account_id}/eventregistrations/{registration_id}'
    response = api.execute_request(request_url)
    return response

# Function to fetch all events with pagination
def get_all_events(max_events=None):
    events = []
    limit = 100
    offset = 0

    while True:
        params = {'$top': limit, '$skip': offset}
        request_url = f'https://api.wildapricot.org/v2.3/accounts/{account_id}/events?' + urllib.parse.urlencode(params)
        response = api.execute_request(request_url)
        new_events = getattr(response, 'Events', [])

        if not new_events:
            break

        events.extend(new_events)
        offset += limit

        # Check if the maximum number of events has been reached
        if max_events and len(events) >= max_events:
            events = events[:max_events]
            break

    return events

# Function to convert ApiObject to a readable dictionary
def api_object_to_readable_dict(api_object):
    if isinstance(api_object, WaApi.ApiObject):
        readable_dict = {}
        for key in dir(api_object):
            value = getattr(api_object, key)
            if not key.startswith("__") and not callable(value):
                if isinstance(value, WaApi.ApiObject):
                    readable_dict[key] = api_object_to_readable_dict(value)
                elif isinstance(value, list):
                    readable_dict[key] = [api_object_to_readable_dict(item) if isinstance(item, WaApi.ApiObject) else item for item in value]
                else:
                    readable_dict[key] = value
        return readable_dict
    elif isinstance(api_object, list):
        return [api_object_to_readable_dict(item) for item in api_object]
    else:
        return api_object

# Function to flatten FieldValues into individual columns
def flatten_field_values(field_values):
    flattened = {}
    for field in field_values:
        field_name = field['FieldName']
        field_value = field['Value']
        if isinstance(field_value, dict) and 'Label' in field_value:
            field_value = field_value['Label']
        elif isinstance(field_value, list):
            field_value = ', '.join([item['Label'] if isinstance(item, dict) and 'Label' in item else item for item in field_value])
        flattened[field_name] = field_value
    return flattened

# Function to convert ApiObject instances to dictionaries
def convert_api_objects_to_dicts(api_objects):
    return [api_object_to_readable_dict(obj) for obj in api_objects]

# Function to process ClickedLinks field into separate columns
def process_clicked_links(clicked_links):
    processed_data = {}
    for i, link in enumerate(clicked_links):
        processed_data[f'Url_{i+1}'] = link.get('Url', '')
        processed_data[f'Clicked_{i+1}'] = link.get('Clicked', False)
        processed_data[f'ClicksCount_{i+1}'] = link.get('ClicksCount', 0)
    return processed_data

# Function to process event registration fields and extract relevant information
def process_event_registration_fields(registration_dict):
    # Extract Contact Name
    contact_details = registration_dict.get('Contact')
    if contact_details:
        registration_dict['ContactName'] = contact_details.get('Name')
        registration_dict['ContacId'] = contact_details.get('Id')
        del registration_dict['Contact']

    # Flatten Event Details
    event_details = registration_dict.get('Event')
    if event_details:
        for key, value in event_details.items():
            if isinstance(value, dict) and 'Label' in value:
                event_details[key] = value['Label']
            registration_dict[f'Event_{key}'] = value
        del registration_dict['Event']

    # Extract Invoice Id
    invoice_details = registration_dict.get('Invoice')
    if invoice_details:
        registration_dict['InvoiceId'] = invoice_details.get('Id')
        del registration_dict['Invoice']

    # Flatten Registration Fields
    registration_fields = registration_dict.get('RegistrationFields', [])
    if registration_fields:
        for field in registration_fields:
            field_name = field['FieldName']
            field_value = field['Value']
            if isinstance(field_value, dict) and 'Label' in field_value:
                field_value = field_value['Label']
            elif isinstance(field_value, list):
                field_value = ', '.join([extract_label(item) for item in field_value])
            registration_dict[field_name] = field_value
        del registration_dict['RegistrationFields']

    # Extract Registration Type Name
    registration_type = registration_dict.get('RegistrationType')
    if registration_type:
        registration_dict['RegistrationTypeName'] = registration_type.get('Name')
        del registration_dict['RegistrationType']

    return registration_dict

def extract_label(item):
    if isinstance(item, dict) and 'Label' in item:
        return item['Label']
    return item

def print_api_object_details(api_object):
    if isinstance(api_object, WaApi.ApiObject):
        for key, value in api_object.__dict__.items():
            if isinstance(value, WaApi.ApiObject) or isinstance(value, list):
                print(f"{key}: (complex type)")
            else:
                print(f"{key}: {value}")
    else:
        print(api_object)

def extract_name(item):
    if isinstance(item, dict) and 'Name' in item:
        return item['Name']
    return item

# Function to check for PII and replace with True/False
def replace_pii_with_boolean(df):
    # Columns to check for PII
    pii_columns = [
        # 'Email', 'Phone', 'Address', 'Address 2', 'Physical Address', 'Office Phone',
        # 'Office Fax', 'Alternate Phone', 'Email Alternate', 'Notes', 'StaffComments', 'EandO Email', 'Farmers Email Address'
    ]
    
    # Replace PII with True/False
    for column in pii_columns:
        if column in df.columns:
            df[column] = df[column].apply(lambda x: False if pd.isnull(x) or x == '' else True)
    
    return df



contact_data = pd.DataFrame(columns=[
    'ID', 'First name', 'Last name', 'Email', 'Archived', 'Donor', 'Event registrant', 
    'Member', 'Suspended member', 'Event announcements', 'Member emails and newsletters', 
    'Email delivery disabled', 'Email delivery disabled automatically', 'Receiving emails disabled', 
    'Balance', 'Total donated', 'Profile last updated', 'Profile last updated by', 'Creation date', 
    'Last login date', 'Administrator role', 'Notes', 'Terms of use accepted', 'Subscription source', 
    'User ID', 'Company', 'Phone', 'Email Alternate', 'Address', 'Address 2', 'City', 'State', 
    'Postal Code', 'Physical Address', 'Office Phone', 'Office Fax', 'Alternate Phone', 
    'Alternate Phone Type', 'MembershipNumber', 'Facebook', 'LinkedIn', 'Twitter', 'Chapter Number', 
    'Member since', 'Renewal due', 'Membership level ID', 'Access to profile by others', 
    'Level last changed', 'Membership status', 'Membership enabled', 'Group participation', 
    'Farmers Email Address', 'Farmers Agent Website', 'Designations', 'How did you hear about UFAA?', 
    'Internal Comments', 'ScrapeSort', 'Action', 'EandONote', 'UFAAdbaStatus', 'CleanupNote', 
    'unsubscribe', 'PreferredName', 'FarmersAppointmentDate', 'AMP', 'AMPAllocation', 'EandOCustomerID', 
    'HomeAddress', 'StaffComments', 'EmailReturnedReason', 'DateAdded'
])


def main():
    recipient_data = []
    email_data = []
    event_data = []
    event_registration_data = []
    store_order_data = []

    # Fetch contacts
    contacts = get_contacts()
    contact_identifiers = getattr(contacts, 'FieldValues', [])
    print(contact_identifiers)


    for contact_id in contact_identifiers:
        try:
            # Get contact information for the current contact ID
            contact_info = get_contact_info(contact_id)
            print(contact_id)
            
            # Get the recipients of the contact, or an empty list if it's not available
            contact_recipients = getattr(contact_info, 'Recipients', [])
            
            # Convert ApiObject instances to dictionaries
            for recipient in contact_recipients:
                recipient_dict = {}
                for key in dir(recipient):
                    value = getattr(recipient, key)
                    if not key.startswith("__") and not callable(value):
                        recipient_dict[key] = value
                recipient_data.append(recipient_dict)
            
        except Exception as e:
            # Handle the case where an error occurs
            print(f"Error processing contact ID {contact_id}: {e}")
            
    # Convert the list of dictionaries to a DataFrame
    recipient_df = pd.DataFrame(recipient_data)

    # Fetch all emails
    try:
        emails = get_all_emails()
        if not emails:
            print("No emails found.")
    except Exception as e:
        print(f"Error fetching emails: {e}")
        return

    email_data = []

    # Directly use email identifiers
    email_identifiers = emails

    for email_id in email_identifiers:
        try:
            email_stats_response = get_account_emails_stats(email_id)
            email_recipients = getattr(email_stats_response, 'Recipients', [])

            for recipient in email_recipients:
                recipient_dict = {}
                for key in dir(recipient):
                    value = getattr(recipient, key)
                    if not key.startswith("__") and not callable(value):
                        recipient_dict[key] = value
                recipient_dict['EmailId'] = email_id

                # Process ClickedLinks
                clicked_links = recipient_dict.get('ClickedLinks', [])
                if clicked_links:
                    clicked_links_dicts = convert_api_objects_to_dicts(clicked_links)
                    recipient_dict.update(process_clicked_links(clicked_links_dicts))
                    del recipient_dict['ClickedLinks']  # Remove the original ClickedLinks field

                email_data.append(recipient_dict)

        except Exception as e:
            print(f"Error processing email ID {email_id}: {e}")

    # Check if email data is empty
    if not email_data:
        print("Email data is empty")


    # Fetch events
    # events = get_all_events(max_events=10)
    events = get_all_events()
    for event in events:
        event_dict = api_object_to_readable_dict(event)
        event_data.append(event_dict)
        event_id = event_dict['Id']
        
        # Fetch event registrations
        try:
            event_registrations_response = get_event_registrations(account_id, event_id)
            if event_registrations_response is None:
                print(f"No response received for event ID {event_id}.")
            elif not event_registrations_response:
                print(f"No registrations found for event ID {event_id}.")
            else:
                for idx, registration in enumerate(event_registrations_response):
                    registration_dict = api_object_to_readable_dict(registration)
                    registration_id = registration_dict['Id']

                    registration_dict = process_event_registration_fields(registration_dict)

                    # Remove the original Event, Contact, RegistrationFields, and RegistrationType columns
                    if 'Event' in registration_dict:
                        del registration_dict['Event']
                    if 'Contact' in registration_dict:
                        del registration_dict['Contact']
                    if 'RegistrationFields' in registration_dict:
                        del registration_dict['RegistrationFields']
                    if 'RegistrationType' in registration_dict:
                        del registration_dict['RegistrationType']

                    try:
                        registration_details_response = get_event_registration_details(account_id, registration_id)
                        registration_details_dict = api_object_to_readable_dict(registration_details_response)
                        registration_details_dict.update(registration_dict)  # Merge details with processed dict
                        registration_details_dict['EventId'] = event_id
                        event_registration_data.append(registration_details_dict)
                    except Exception as e:
                        print(f"Error fetching registration details for registration ID {registration_id}: {e}")

        except Exception as e:
            print(f"Error processing event registrations for event ID {event_id}: {str(e)}")



    # Create DataFrames
    recipient_df = pd.DataFrame(recipient_data)
    email_df = pd.DataFrame(email_data)
    event_df = pd.DataFrame(event_data)
    event_registration_df = pd.DataFrame(event_registration_data)

    # Drop specified columns from event_registration_df
    event_registration_df = event_registration_df.drop(columns=['Contact', 'Event', 'RegistrationFields', 'RegistrationType', 'Invoice'], errors='ignore')

    # Replace PII with True/False
    recipient_df = replace_pii_with_boolean(recipient_df)
    event_registration_df = replace_pii_with_boolean(event_registration_df)
    email_df = replace_pii_with_boolean(email_df)

    # Save DataFrames to an Excel workbook with separate sheets
    with pd.ExcelWriter('wild_apricot_data.xlsx') as writer:
        recipient_df.to_excel(writer, sheet_name='Contacts', index=False)
        email_df.to_excel(writer, sheet_name='EmailStats', index=False)
        event_df.to_excel(writer, sheet_name='Events', index=False)
        event_registration_df.to_excel(writer, sheet_name='EventRegistrations', index=False)


    print('Data has been written to wild_apricot_data.xlsx')

if __name__ == '__main__':
    main()