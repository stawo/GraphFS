from pyArango.connection import *

conn = Connection(username="root", password="b13lsk0")

for db in conn.databases:
	print (db)

db = conn["Prova"]

print(db)

studentsCollection = db.createCollection(name="Students")
doc1 = studentsCollection.createDocument()
doc1["name"] = "John Smith"
doc2 = studentsCollection.createDocument()
doc2["firstname"] = "Emily"
doc2["lastname"] = "Bronte"
doc1._key = "johnsmith"
doc1.save()
doc2.save()

for student in studentsCollection.fetchAll():
	print(student)
