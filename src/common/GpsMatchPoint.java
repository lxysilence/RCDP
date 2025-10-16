package common;

import com.bmwcarit.barefoot.roadmap.RoadPoint;
import com.esri.core.geometry.Point;

import java.io.Serializable;
import java.sql.Timestamp;

public class GpsMatchPoint implements Serializable {

    public double getX() {
        return x;
    }

    public void setX(double x) {
        this.x = x;
    }

    public double getY() {
        return y;
    }

    public void setY(double y) {
        this.y = y;
    }

    public double getOriginalX() {
        return originalX;
    }

    public void setOriginalX(double originalX) {
        this.originalX = originalX;
    }

    public double getOriginalY() {
        return originalY;
    }

    public void setOriginalY(double originalY) {
        this.originalY = originalY;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public long getRoad() {
        return road;
    }

    public void setRoad(long road) {
        this.road = road;
    }

    //原gps点
    double originalX;
    double originalY;

    long time;

    //匹配点
    double x;
    double y;

    //匹配道路
    long road;

    public GpsMatchPoint(RoadPoint roadPoint) {
        this.x = roadPoint.geometry().getX();
        this.y = roadPoint.geometry().getY();
        this.road = roadPoint.edge().base().refid();
    }

    public GpsMatchPoint(Point originalPoint, long time, RoadPoint matchPoint) {
        this.originalX = originalPoint.getX();
        this.originalY = originalPoint.getY();
        this.time = time;
        this.x = matchPoint.geometry().getX();
        this.y = matchPoint.geometry().getY();
        this.road = matchPoint.edge().base().refid();
    }
}
