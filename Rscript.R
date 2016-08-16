library(Storm)
library(forecast)

myData = read.csv("perf1.csv")[1:100,-1]
head(myData)
storm = Storm$new()
storm$tuple$input = list(cpu=myData$cpu, ram=myData$ram, disk=myData$disk)

storm$lambda = function(s)
{
    t = s$tuple
    s$ack(t)                                # acknowledge receipt of the tuple.
    s$log(c("processing tuple=",t$id))      # log a message.

    limit.v = 50
    limit.p = 0.7
    
    for (i in 1:length(storm$tuple$input))
    {
        dataPred = ts(t$input[[i]])
        fitPred = auto.arima(dataPred)
        point = data.frame(forecast(fitPred, h=1))$Point.Forecast
        se = sqrt(KalmanForecast(n.ahead = 1, fitPred$model)[[2]]*fitPred$sigma2)
        prob = 1-pnorm((limit.v-point)/se)
        warning = prob > limit.p
        myOut = list(point=point, se=se, prob=prob, warning=warning)
        
        t$output = myOut     # create 1st tuple
        s$emit(t)            # emit 1st tuple
    }
}

# enter the main tuple-processing loop.
storm$run()

# for test in R
storm$lambda(storm)
