# flink 提交流程


>流程
>
>1.CliFrontend   
>2.YarnJobClusterEntrypoint
>  > TaskExecutorRunner -> TaskManagerRunner
>














### CliFrontend   
    执行流程
 \> getConfigurationDirectoryFromEnv() -> 加载conf目录
 \> GlobalConfiguration.loadConfiguration(); -> 根据conf目录加载Flink yaml配置文件
 \> loadCustomCommandLines() -> 主要用于后面解析命令行用
        会创建一个ArrayList 一共添加三个CommandLine  按顺序添加
            GenericCLI > FlinkYarnSessionCli > DefaultCLI
        后面会根据 isActive 方法判断是否是活跃的,来进行相关的解析
  \> parseAndRun 用于解析命令行,并请求启动的操作      
            获取args[0]  -> get Action -> 进行witch判断,执行动作
            创建run 对应的默认配置选项 ->  即  -s -p -...
            找到活跃的customCommandLine -> 顺序Generic -> yarn -> default
            中间一些配置文件的封装,执行程序的封装
   \> executeProgram 执行封装好的program
   \> invokeMain(这个不是方法) 最终会执行用户的main方法      
   到这里就开始执行用户的main方法了,也就是需要执行用户的代码,重点在与 env.execute方法生成graph      

    TODO 进入StreamExecutionEnvironment.execute方法的执行 -> 
    
### StreamExecutionEnvironment.execute方法执行          
  \> 生成StreamGraph
  \> 将StreamGraph转换成JobGraph
  \> 启动appMaster
  \> appMaster执行我们传入的入口类,每个执行模式下是不同的 per-job模式启动了YarnJobClusterEntrypoint
  
  TODO 上面是将appMaster启动起来了 在appMaster中执行入口类
  YarnJobClusterEntrypoint >  -   per-job模式的入口类
  即执行该类的main方法
###  YarnJobClusterEntrypoint

  
  





