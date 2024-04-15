#include<iostream>
#include<fstream>
#include<iomanip>
#include<vector>
#include<numeric>
#include<string>
#include<cassert>
#include<cmath>

#ifndef regression_H
#define regression_H
class regression {
public:
    //pair of variates
    std::vector<double> x, y;
    //amount of pa of variates
    int n = 0;
    //coefficients a & b
    double a = 0;
    double b = 0;
    //variables for calculating the square of the deviations
    double sx2 = 0;
    double sy2 = 0;
    double sxy = 0;

public:
    //initialise the protected variables
    regression(const std::vector<double> & x, const std::vector<double> & y) {
        a = b = sx2 = sy2 = sxy = 0;
        this->x = x;
        this->y = y;
        n = x.size();
    }
    //function to check whether a regression line exists or not
    bool coefficients();
    //correlation coefficients according to Parsen's formula
    double correlation();
};

//is there a best fit line available?
//false if not, true if there is
bool regression::coefficients() {

    double sx = 0, sy = 0, mean_x = 0, mean_y = 0.0;
    //sum of x & y values
    sx = std::accumulate(x.begin(), x.end(), sx);
    sy = std::accumulate(y.begin(), y.end(), sy);
    //mean of x & y values
    mean_x = sx/n;
    mean_y = sy/n;
    //sum of x & y squared values; using inner_product() function
    sx2 = std::inner_product(x.begin(), x.end(), x.begin(), sx2);
    sy2 = std::inner_product(y.begin(), y.end(), y.begin(), sy2);
    //sum of x*y values
    sxy = std::inner_product(x.begin(), x.end(), y.begin(), sxy);

    //need to calculate the coefficients a & b for the 
    //linear regression formula: y = bx + a
    double denominator = n*sxy - sx*sx;

    if (denominator != 0) {

        //coefficient a and b
        b = (n*sxy - sx*sy)/denominator;
        //a = sx2*sy - sx*sxy/denominator
        a = mean_y - b*mean_x;

        return true;
        
    } else {

        return false;
    }
    
}


double regression::correlation() {
    //calculate Pearson's correlation coefficient
    double correlation_coefficient = sxy/sqrt(sx2*sy2);
    return correlation_coefficient;
}
#endif