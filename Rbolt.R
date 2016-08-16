################### IGNORE ###################
# myData = read.csv("perf1.csv")[1:100,-1]
# 
# t$input$client = 'Client Name'
# t$input$limit = c(50, 100)
# t$input$time = 'TimeStamp'
# t$input$process = 'ProcessList'
################### IGNORE ###################

library(Storm)
library(forecast)

storm = Storm$new()
storm$lambda = function(s)
{
    t = s$tuple
    s$ack(t)
    
    t$output[[1]] = t$input[[1]]    # clinetname
    t$output[[2]] = t$input[[3]]    # timestamp
    t$output[[3]] = t$input[[4]]    # processlist
    
    limit.v = t$input[[2]]          # limit
    limit.p = 0.7
    
    cpuV = rep(0, 10)
    ramV = rep(0, 10)
    for (i in 0:10)
    {
        cpuV[i+1] = as.numeric(t$input[i*2+5]) # cpu1, ...
        ramV[i+1] = as.numeric(t$input[i*2+6]) # ram1, ...
    }

    dataPred = ts(cpuV)
    fitPred = auto.arima(dataPred)
    point = data.frame(forecast(fitPred, h=1))$Point.Forecast
    se = sqrt(KalmanForecast(n.ahead = 1, fitPred$model)[[2]]*fitPred$sigma2)
    prob = 1-pnorm((limit.v[1]-point)/se)
    warning = prob > limit.p
    
    t$output[4] = as.character(point)
    t$output[5] = as.character(se)
    t$output[6] = as.character(prob)
    t$output[7] = as.character(warning)
    
    dataPred = ts(ramV)
    fitPred = auto.arima(dataPred)
    point = data.frame(forecast(fitPred, h=1))$Point.Forecast
    se = sqrt(KalmanForecast(n.ahead = 1, fitPred$model)[[2]]*fitPred$sigma2)
    prob = 1-pnorm((limit.v[2]-point)/se)
    warning = prob > limit.p
    
    t$output[8] = as.character(point)
    t$output[9] = as.character(se)
    t$output[10] = as.character(prob)
    t$output[11] = as.character(warning)
    
    s$emit(t)
}

# enter the main tuple-processing loop.
storm$run()

# storm$lambda(storm)
