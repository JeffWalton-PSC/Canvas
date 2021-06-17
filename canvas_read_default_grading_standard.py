import os

# Import the Canvas class
from canvasapi import Canvas

SELECTED_TERM = "2021.Fall"

# Canvas API URL
API_URL = os.environ.get("CANVAS-BETA_API_URL")
if not API_URL:
    raise KeyError("CANVAS_API_URL is not set.")
print(f"API_URL: {API_URL}")
# Canvas API key
API_KEY = os.environ.get("CANVAS-BETA_API_TOKEN")
if not API_KEY:
     raise KeyError("CANVAS_API_TOKEN is not set.")

# Initialize a new Canvas object
canvas = Canvas(API_URL, API_KEY)

account = canvas.get_account(1)
print(f"account(1): {account}")
for g in account.get_grading_standards():
    print(f"\taccount grading_standard: {g}")

academic_account = canvas.get_account(126)
print(f"academic_account(126): {academic_account}")
for g in academic_account.get_grading_standards():
    print(f"\tacademic_account grading_standard: {g}")

academic_accounts = academic_account.get_subaccounts()
for dept in academic_accounts:
    print(f"\ndept: {dept}")
    # for g in dept.get_grading_standards():
    #     print(f"\tdept grading_standard: {g}")
    for c in dept.get_courses(include=['term']):
        term = c.term['sis_term_id']
        if term == SELECTED_TERM:
            print(c, term)
            for cg in c.get_grading_standards():
                print(f"\tcourse grading_standard: {cg}")