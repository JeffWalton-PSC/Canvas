import os

# Import the Canvas class
from canvasapi import Canvas

# Canvas API URL
API_URL = os.environ.get("CANVAS-BETA_API_URL")
if not API_URL:
    raise KeyError("CANVAS_API_URL is not set.")
print(API_URL)
# Canvas API key
API_KEY = os.environ.get("CANVAS-BETA_API_TOKEN")
if not API_KEY:
     raise KeyError("CANVAS_API_TOKEN is not set.")

# Initialize a new Canvas object
canvas = Canvas(API_URL, API_KEY)

courses = canvas.get_courses()
print(courses[0])

# Grab course
course = canvas.get_course(436)

# Access the course's name
print(course.name)
print(course)

accounts = canvas.get_accounts()
print(accounts)

print(accounts[0])

for account in accounts:
    print(account)
    for g in account.get_grading_standards():
        print(g)

# i=0
for c in accounts[0].get_courses(include=['term'])[:10]:
    print(c, c.term)
    for g in c.get_grading_standards():
        print(g)