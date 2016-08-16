library(Storm)
library(forecast)

myData = read.csv("perf1.csv")
storm = Storm$new()

storm$lambda = function(s)
{
    t = s$tuple
    t$input = list(myData$cpu[1:100])
    s$ack(t)                                # acknowledge receipt of the tuple.
    s$log(c("processing tuple=",t$id))      # log a message.

    limit.v = 50
    limit.p = 0.7
    
    dataPred = ts(t$input[[1]])
    fitPred = auto.arima(dataPred)
    point = data.frame(forecast(fitPred, h=1))$Point.Forecast
    se = sqrt(KalmanForecast(n.ahead = 1, fitPred$model)[[2]]*fitPred$sigma2)
    prob = 1-pnorm((limit.v-point)/se)
    warning = prob > limit.p
    myOut = list(point=point, se=se, prob=prob, warning=warning)
    
    t$output = myOut     # create 1st tuple
    s$emit(t)            # emit 1st tuple
    
    s$fail(t) #alternative/optional: mark the tuple as failed.
}

# enter the main tuple-processing loop.
storm$run()

# for test in R
storm$lambda(storm)
