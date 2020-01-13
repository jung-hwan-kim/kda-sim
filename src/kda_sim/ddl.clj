(ns kda-sim.ddl
  (:require [camel-snake-kebab.core :as csk]
            [taoensso.nippy :as nippy]
            [next.jdbc :as jdbc]
            [honeysql.core :as sql]
            [honeysql.helpers :refer :all :as helpers]
            [cheshire.core :as json]))


(def data1 {:vehicleId 445650969,
            :eventType "vehicleUpdated",
            :eventDate "2020-01-08T00:00:23.949Z",
            :country 1,
            :runNumber "182",
            :lotNumber "C",
            :lane "",
            :iteration nil,
            :currentHighBid 0,
            :buyNowPrice 0,
            :auctionEndDate "2020-01-15T07:00:00.000Z",
            :vehicleGrade 4.1,
            :drivetrain "Front Wheel Drive",
            :sellerAnnouncements "14 DAY SELLERS GUARANTEE;TDI AEM-EXT WTY;",
            :hasPriorPaintwork false,
            :titleState "MI",
            :fuelType "Diesel",
            :vehicleDetailUrl "https://buy.adesa.com/openauction/detail.html?vehicleId=445650969&entryway=drivin",
            :mileage 32241,
            :vehicleType "Not Provided",
            :bodyStyleName "4DR SDN FWD",
            :imageViewerUrl "https://buy.adesa.com/openauction/pictureBrowser.html?vehicleId=445650969&index=-1",
            :engineName "2.0L 4CYL DIESEL FUEL",
            :sellerType "Manufacturer",

            :inspectionDate "2019-02-12T05:14:30.000Z",
            :primaryImageUrl "http://adesa.kar-media.com/display.php?img=rStQYqAJ8MQQSyIRqyqobsxe4oJOBnJnIVS4iO1M0F8.jpg",
            :saleEventDate "2020-01-14T08:00:00.000Z",
            :salvage false,
            :vin "WAUCJGFF5F1045207",
            :finalPrice ""
            :inspectionComments "",
            :basePrice "",

 :chromeTrim "2.0 TDI PREMIUM PLUS",
 :thirdPartyCRExists true,
 :priceReduced nil,
 :autoCheck "",
 :remotes "0",
 :rlLbListingCategory "Standard",
 :isAutoGrade true,
 :openEqualsReserve nil,
 :auctionVendor "ADESA Salt Lake",
 :structuralDamage "No",
 :odors "None",
 :runlistDealerCodes ["0"],
 :seriesName "2.0 TDI PREMIUM PLUS",
 :state "UT",
 :inTransitIndicator false,
 :driveable "Yes",
 :chromeYear 2015,
 :modelName "A3",
 :assetType "Car/Light Truck",
 :interiorColor "Black",
 :saleEventType "Consignment Sale",
 :liveblockDealerCodes [],
 :adesaAssurance false,
 :makeName "AUDI",
 :odometerCondition "Functions Properly",
 :manuals "Missing",
 :unitOfMeasure "Miles",
 :year 2015,
 :sellerName "Volkswagen Group of America",
 :keys "1",
 :frameDamage 0,
 :postalCode "84104",
 :transmission "Automatic Transmission",
 :psi nil,
 :cylinders 4,
 :chromeMake "AUDI",
 :lastRun nil,
 :chromeModel "A3",
 :inspectionCompany "ADESA",
 :chromeStyleId 370880,
 :dealerblockDealerCodes [],
 :programSegment "ADESA/OL",
 :exteriorColor "Black",
 :engineDisplacement 2,
 :pdi nil,
 :dealerblockListingCategory "",
 :firstRun false,
 })




(defn create-event-table [conn]
  (jdbc/execute! conn ["create table event (
  id int auto_increment primary key,
  vehicleId int,
  eventDate varchar(32),
  eventType varchar(32),
  country int,
  runNumber varchar(10),
  lotNumber varchar(10),
  lane varchar(10),
  iteration int,
  currentHighBid int,
  buyNowPrice int,
  auctionEndDate varchar(32),
  vehicleGrade decimal(3,2),
  drivetrain varchar(32),
  sellerAnnouncements varchar(4000),
  hasPriorPaintwork boolean,
  titleState varchar(4),
  fuelType varchar(32),
  vehicleDetailUrl varchar(128),
  mileage int,
  vehicleType varchar(32),
  bodyStyleName varchar(48),
  imageViewerUrl varchar(128),
  engineName varchar(64),
  sellerType varchar(64),
  inspectionDate varchar(32),
  primaryImageUrl varchar(124),
  saleEventDate varchar(32),
  salvage boolean,
  vin varchar(32),
  finalPrice varchar(16),
  inspectionComments varchar(4000),
  basePrice varchar(16)
)"]))



(defn drop-table [conn]
    (next.jdbc/execute! conn ["drop table event"]))

(defn select-event [conn limit]
    (next.jdbc/execute! conn [(str "select * from event limit " limit)]))


(defn delete-table [conn]
  (next.jdbc/execute! conn ["delete event"]))

(defn add-index[conn]
  (jdbc/execute! conn ["create index evendate_index on event (eventdate asc) "]))