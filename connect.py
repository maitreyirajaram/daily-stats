import pyodbc
import argparse
import boto3
import datetime
import googlemaps 
import math
#import reverse_geocode ###python offline geocoder
import csv

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from io import open
from email.mime.application import MIMEApplication


# make_output_files: connects to live database, runs SQL queries and writes results to files
#send_mail: creates and sends email with all file attachments 
#reverse-geolocation 
#maps with pinned locations for daily live (active) users -- in body of email

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("password", type = str,help= "password to connect to server")
    #parser.add_argument("emailAddress", type=str, help = "full email address")
    #parser.add_argument("emailPassword",type=str, help = "email password" )
    arg = parser.parse_args()
    passwordServer = arg.password
    #addressEmail = arg.emailAddress
    #passwordEmail = arg.emailPassword
    TupleNewUsers = make_output_files(passwordServer)
    noNewUsers = str(TupleNewUsers[0])
    #print noNewUsers
    locNewUser=str(TupleNewUsers[1])
    LNNewUser = str(TupleNewUsers[2])
    #print "locNewUser " + locNewUser
    #print "LNNewUser " + LNNewUser
    #send_mail(noNewUsers, locNewUser, LNNewUser)
    #send_mail2(addressEmail, passwordEmail) # sends automatic emails

def create_attachment(filepath):
    # attach all files here
    attachment = open(filepath, encoding='utf-8').read()  # path of file must be updated for AWS scheduler
    fileMIMEForm = MIMEApplication(attachment.encode("utf-8"))
    # fileMIMEForm = MIMEBase('application', 'octet-stream')
    # fileMIMEForm.set_payload(attachment)
    # encoders.encode_base64(fileMIMEForm)
    fileMIMEForm.add_header('Content-Disposition', 'attachment', filename=(filepath.split('/'))[4])
    return fileMIMEForm


def send_mail(noNewUsers, locNewUser, LNNewUser, fromEmail, toEmail):
    client = boto3.client('ses')
    ### email contents
    msg = MIMEMultipart()
    msg['FROM'] = fromEmail
    msg['To'] = toEmail
    msg['Subject'] = "Your Stats for Today-GOOGLE API TEST"
    body1 = "Welcome to your daily app report, delivered through your AWS (SES) account and connected to your MSSQL database.\n\nAttached are files containing results from your daily SQL queries. Includes registered accounts per city, new user account data, and top live destinations for the day.\n\nNumber of New Users: "
    body2 = "\nTop Home Country for New Users: "
    body3 = " new users come from "
    body4 = "\n\nThis program is currently set to fetch data at: 2:00 PM (EST).\n(C) 2019 Maitreyi Rajaram"
    body = body1 + noNewUsers + body2 + LNNewUser + body3 + locNewUser + body4
    msg.preamble = 'Multipart message.\n'
    msg.attach(MIMEText(body, 'plain'))


    msg.attach(create_attachment("../daily-app-stats/DailyReport.csv"))
    msg.attach(create_attachment("../daily-app-stats/NewUserInfo.csv"))

    result = client.send_raw_email(RawMessage={'Data': msg.as_string().encode("utf-8")},Source=msg['From'],
                                   Destinations=[msg['To']])


def make_output_files(argpass): #makes out put files and returns number of new users
    connstr = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=tcp:18.222.96.225;DATABASE=travellersconnect;' \
            'UID=jetzy-webservices;PWD=' + argpass
    cnx = pyodbc.connect(connstr)
    cursor = cnx.cursor()
    query = ("Select TOP 50 HomeTownCity, COUNT(HomeTownCity)AS Frequency FROM dbo.Users GROUP BY "
             "HomeTownCity ORDER BY COUNT(HomeTownCity) desc")
    cursor.execute(query)
    row = cursor.fetchone()
    f = open("DailyReport.csv", "w+", encoding="utf-8") # change to csv
    f.write(u"Daily Report\n")
    f.write(u"\nTop 50 Hometown Cities\t\t\t\tNo of Total Users\n\n")
    while row:
        f.write(u"{0:24}\t{1:28}\n".format(row[0], row[1]))
        row = cursor.fetchone()

    # number of new users -- returned from this method, given to sendmail() to put in body of email
    dateandTime = str(datetime.datetime.now())
    currentDate = dateandTime.split(' ')[0]

    #query for new user locations
    queryNewUserL1 = "SELECT HomeTownCountry, COUNT(HomeTownCountry)AS Frequency from dbo.users where createddate >= CONVERT(DATETIME, '"
    queryNewUserL2 = "') GROUP BY HomeTownCountry ORDER BY COUNT(HomeTownCountry) desc"
    queryNewUserL= queryNewUserL1+currentDate+queryNewUserL2
    cursor.execute(queryNewUserL)
    row3 = cursor.fetchone()
    NewUserL= row3[0]
    NewUserLN= row3[1]
    f.write(u"\n\nYour New Users are From...\n\n")
    f.write(u"Home Location\t\t\t\t\tNo of New Users\n\n")
    while row3:
        f.write(u"{0:24}\t{1:28}\n".format(row3[0], row3[1]))
        row3=cursor.fetchone()

    # query for live locations
    f.write(u"\n\nTop Destinations For Today: Based on Live Location Sharing\n")
    f.write(u"Location, No of Active Users\n")
    #f.write(u"REVERSE GEOLOCATION USING GOOGLE API IN PROGRESS, for now lat/long displayed\n")

    # lat/long conversion --Google API
    #gmaps = googlemaps.Client(key='API_KEY')

    queryLivep1 = "SELECT Latitude, Longitude from dbo.UserGeoLocationLog where UpdatedOn >= CONVERT(DATETIME, '" #add userid to query
    queryLivep2 = "')"
    queryLive = queryLivep1+currentDate+queryLivep2
    cursor.execute(queryLive)
    rowLive= cursor.fetchone() #separate coordinates from UserID
    geocodeCounter = 0
    while rowLive:
        # reverse geocoding -- not on scheduler, only to be run manually when required
        #coordinates = ((41.63689319979552, -87.51736460261746))
        #geocode_result = gmaps.reverse_geocode(rowLive)
        # print geocode_result.address_components
        #address = geocode_result[0]['formatted_address']
        #f.write(address)
        #f.write(u"lat{0},long{1}".format(rowLive[0], rowLive[1]))
        #f.write(u"\n")
        #rowLive=cursor.fetchone()

        #reduces number of reverse geocoding requests by checking for similar coordinates (likely same address)
        #calculate distance between prev coordinate and current coordinate
        firstLat = rowLive[0]
        firstLong = rowLive[1]
        coordinates = rowLive
        rowLive = cursor.fetchone()

        if rowLive == None:
            break

        secondLat = rowLive[0]
        secondLong = rowLive[1]
        distLat = math.radians(secondLat-firstLat)
        distLong = math.radians(secondLong-firstLong)
        a = math.sin(distLat/2)*math.sin(distLat/2) + math.cos(math.radians(firstLat))*math.cos(math.radians(secondLat))*math.sin(distLong/2)*math.sin(distLong/2)
        distance = (6371)* (2 * math.atan2(math.sqrt(a),math.sqrt(1-a))) #unit : kilometers

        # print distances --test
        floatDistance = str(distance)
        f.write(u"Distance:{0}".format(floatDistance))
        f.write(u"\t{0} {1},{2} {3}\n".format(firstLat, firstLong, secondLat, secondLong))


        ###reverse geo for first entry if distance is greater than 5 kilometers
        if distance >= 5:
            ###reverse geocoding
            geocodeCounter+=1
            #geocode_result = gmaps.reverse_geocode(coordinates)
            #address = geocode_result[0]['formatted_address']
            #f.write(u"Address: \n".format(address))

        ###





    print geocodeCounter

    f.close()

    query2p1 = "SELECT COUNT(*) from dbo.users where createddate >= CONVERT(DATETIME, '"
    query2p2 = "')"
    query2 = query2p1 + currentDate + query2p2
    cursor.execute(query2)
    newUsers = cursor.fetchone()[0]

    #put into new users file
    query3p1 = "SELECT FirstName, Lastname, HomeTownCountry, UserAbout from dbo.users where createddate >= CONVERT(DATETIME, '"
    query3p2 = "')"
    query3 = query3p1 + currentDate + query3p2
    cursor.execute(query3)
    row2 = cursor.fetchone()
    print row2
    #f2 = open("NewUserInfo.csv", "w+", encoding = "utf-8")
    with open("NewUserInfo.csv", newline='')as f2:
        writer = csv.writer(f2)
        writer.writerows(u"New Users(First Name, Last Name), Country of Residence, Bio (if applicable)\n\n")
        while row2:
            #for item in row2:
            writer.writerows(row2)
            #f2.write(u"{0:24}\t".format(item))
            f2.write(u"\n\n")
            row2 = cursor.fetchone()
        f2.close()





    cursor.close()
    cnx.close()
    return newUsers, NewUserL, NewUserLN


if __name__ == '__main__':
    main()
