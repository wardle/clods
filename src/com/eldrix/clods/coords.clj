(ns com.eldrix.clods.coords
  (:import (org.geotools.referencing ReferencingFactoryFinder)
           (org.geotools.referencing.operation DefaultCoordinateOperationFactory)
           (org.geotools.geometry GeneralDirectPosition)))

(defn wgs84->osgb36
  [latitude longitude]
  (let [factory (ReferencingFactoryFinder/getCRSAuthorityFactory "EPSG" nil)
        wgs84crs (.createCoordinateReferenceSystem factory "4326") ;; WGS84
        osgbCrs (.createCoordinateReferenceSystem factory "27700") ;; OSGB36
        op-factory (DefaultCoordinateOperationFactory.)
        wgs84->osgb (.createOperation op-factory wgs84crs osgbCrs)
        pos (GeneralDirectPosition. latitude longitude)
        result (.getCoordinate (.transform (.getMathTransform wgs84->osgb) pos pos))]
    {:OSGB36/easting (aget result 0) :OSGB36/northing (aget result 1)}))

(defn osgb36->wgs84
  [easting northing]
  (let [factory (ReferencingFactoryFinder/getCRSAuthorityFactory "EPSG" nil)
        wgs84crs (.createCoordinateReferenceSystem factory "4326") ;; WGS84
        osgbCrs (.createCoordinateReferenceSystem factory "27700") ;; OSGB36
        op-factory (DefaultCoordinateOperationFactory.)
        osgb->wgs84 (.createOperation op-factory osgbCrs wgs84crs)
        pos (GeneralDirectPosition. easting northing)
        result (.getCoordinate (.transform (.getMathTransform osgb->wgs84) pos pos))]
    {:urn.ogc.def.crs.EPSG.4326/latitude (aget result 0) :urn.ogc.def.crs.EPSG.4326/longitude (aget result 1)}))

(comment
  (osgb36->wgs84 317551 179319))

