using System;
using System.Threading.Tasks;

using Quartz;
using Quartz.Impl;
using Quartz.Logging;

using Npgsql;

namespace LogTostat
{
    class Program
    {
        public static int cnt = 1;
        private static async Task Main(string[] args)
        {
            // Grab the Scheduler instance from the Factory
            StdSchedulerFactory factory = new StdSchedulerFactory();
            IScheduler scheduler = await factory.GetScheduler();

            // and start it off
            await scheduler.Start();

            var INTERVAL_SECONDS = 300;

            // define the job and tie it to our HelloJob class
            IJobDetail job = JobBuilder.Create<StatJob>()
                .WithIdentity("job1", "group1")
                .Build();

            // Trigger the job to run now, and then repeat every 10 seconds
            ITrigger trigger = TriggerBuilder.Create()
                .WithIdentity("trigger1", "group1")
                .StartNow()
                .WithSimpleSchedule(x => x
                    .WithIntervalInSeconds(INTERVAL_SECONDS)
                    .RepeatForever())
                .Build();

            // Tell quartz to schedule the job using our trigger
            await scheduler.ScheduleJob(job, trigger);

            // some sleep to show what's happening
            await Task.Delay(TimeSpan.FromSeconds(999999)); 

            // and last shut down the scheduler when you are ready to close your program
            await scheduler.Shutdown();

            Console.WriteLine("Press any key to close the application");
            Console.ReadKey();
        }

        // simple log provider to get something to the console
        private class ConsoleLogProvider : ILogProvider
        {
            public Logger GetLogger(string name)
            {
                return (level, func, exception, parameters) =>
                {
                    if (level >= LogLevel.Info && func != null)
                    {
                        Console.WriteLine("[" + DateTime.Now.ToLongTimeString() + "] [" + level + "] " + func(), parameters);
                    }
                    return true;
                };
            }

            public IDisposable OpenNestedContext(string message)
            {
                throw new NotImplementedException();
            }

            public IDisposable OpenMappedContext(string key, object value, bool destructure = false)
            {
                throw new NotImplementedException();
            }
        }
    }

    public class StatJob : IJob
    {
        public Task Execute(IJobExecutionContext context)
        {
            var cs = "Host=211.47.177.137; Port=7575;User ID=postgres;Password=qwe!23;Database=GStream_System";

            using var con = new NpgsqlConnection(cs);
            con.Open();

            var sql = @"INSERT INTO customer_history(customer_id, pc_count, YEAR, MONTH, DAY, HOUR)
                                SELECT con.customer_id, COUNT(client_pc_id), SUBSTRING(TO_CHAR(get_time_unix(get_unix_time(null)),'YYYY-MM-DD HH24:MI:SS'),1,4)
		                                , SUBSTRING(to_char(get_time_unix(get_unix_time(null)), 'YYYY-MM-DD HH24:MI:SS'), 6, 2 )  
		                                , SUBSTRING(to_char(get_time_unix(get_unix_time(null)), 'YYYY-MM-DD HH24:MI:SS'), 9, 2 )  
		                                , SUBSTRING(to_char(get_time_unix(get_unix_time(null)), 'YYYY-MM-DD HH24:MI:SS'), 12, 2 ) 
                                FROM   connection_log con
                                       INNER JOIN (SELECT  b.customer_id
						                                FROM customer a
						                                     LEFT OUTER join (						
								                                SELECT customer_id
								                                FROM   customer_history
								                                WHERE YEAR = SUBSTRING(TO_CHAR(get_time_unix(get_unix_time(null)),'YYYY-MM-DD HH24:MI:SS'),1,4)
								                                AND   MONTH = SUBSTRING(to_char(get_time_unix(get_unix_time(null)), 'YYYY-MM-DD HH24:MI:SS'), 6, 2 )  
								                                AND   DAY   = SUBSTRING(to_char(get_time_unix(get_unix_time(null)), 'YYYY-MM-DD HH24:MI:SS'), 9, 2 )  
								                                AND   HOUR  = SUBSTRING(to_char(get_time_unix(get_unix_time(null)), 'YYYY-MM-DD HH24:MI:SS'), 12, 2 ) 
								                                ) b ON a.customer_id = b.customer_id AND b.customer_id IS NULL
						                                ) us ON con.customer_id = us.customer_id
                                WHERE  SUBSTRING(TO_CHAR(get_time_unix(start_date),'YYYY-MM-DD HH24:MI:SS'),1,4) = SUBSTRING(TO_CHAR(get_time_unix(get_unix_time(null)),'YYYY-MM-DD HH24:MI:SS'),1,4)
                                AND   SUBSTRING(TO_CHAR(get_time_unix(start_date),'YYYY-MM-DD HH24:MI:SS'),1,4) = SUBSTRING(to_char(get_time_unix(get_unix_time(null)), 'YYYY-MM-DD HH24:MI:SS'), 6, 2 )  
                                AND   SUBSTRING(TO_CHAR(get_time_unix(start_date),'YYYY-MM-DD HH24:MI:SS'),1,4)   = SUBSTRING(to_char(get_time_unix(get_unix_time(null)), 'YYYY-MM-DD HH24:MI:SS'), 9, 2 )  
                                AND   SUBSTRING(TO_CHAR(get_time_unix(start_date),'YYYY-MM-DD HH24:MI:SS'),1,4)  = SUBSTRING(to_char(get_time_unix(get_unix_time(null)), 'YYYY-MM-DD HH24:MI:SS'), 12, 2 ) 

                                GROUP BY con.customer_id
                                     , SUBSTRING(TO_CHAR(get_time_unix(get_unix_time(null)),'YYYY-MM-DD HH24:MI:SS'),1,4)
		                             , SUBSTRING(to_char(get_time_unix(get_unix_time(null)), 'YYYY-MM-DD HH24:MI:SS'), 6, 2 )  
		                             , SUBSTRING(to_char(get_time_unix(get_unix_time(null)), 'YYYY-MM-DD HH24:MI:SS'), 9, 2 )  
		                             , SUBSTRING(to_char(get_time_unix(get_unix_time(null)), 'YYYY-MM-DD HH24:MI:SS'), 12, 2 ) 
                                ;
                      
	                    UPDATE customer_history a
	                    SET pc_count = b.cnt
	                    FROM (select customer_id
					                    , count(client_pc_id) cnt
					                    , SUBSTRING(TO_CHAR(get_time_unix(get_unix_time(null)),'YYYY-MM-DD HH24:MI:SS'),1,4)
					                    , SUBSTRING(to_char(get_time_unix(get_unix_time(null)), 'YYYY-MM-DD HH24:MI:SS'), 6, 2 )  
	                                    , SUBSTRING(to_char(get_time_unix(get_unix_time(null)), 'YYYY-MM-DD HH24:MI:SS'), 9, 2 )  
	                                    , SUBSTRING(to_char(get_time_unix(get_unix_time(null)), 'YYYY-MM-DD HH24:MI:SS'), 12, 2 ) 
			                    from connection_log 		
			                    GROUP BY customer_id
					                    , SUBSTRING(TO_CHAR(get_time_unix(get_unix_time(null)),'YYYY-MM-DD HH24:MI:SS'),1,4)
					                    , SUBSTRING(to_char(get_time_unix(get_unix_time(null)), 'YYYY-MM-DD HH24:MI:SS'), 6, 2 )  
	                                    , SUBSTRING(to_char(get_time_unix(get_unix_time(null)), 'YYYY-MM-DD HH24:MI:SS'), 9, 2 )  
	                                    , SUBSTRING(to_char(get_time_unix(get_unix_time(null)), 'YYYY-MM-DD HH24:MI:SS'), 12, 2 ) 
	                                ) AS B			
	                    WHERE year =  SUBSTRING(TO_CHAR(get_time_unix(get_unix_time(null)),'YYYY-MM-DD HH24:MI:SS'),1,4)
	                    AND   month = SUBSTRING(to_char(get_time_unix(get_unix_time(null)), 'YYYY-MM-DD HH24:MI:SS'), 6, 2 )  
	                    AND   day   = SUBSTRING(to_char(get_time_unix(get_unix_time(null)), 'YYYY-MM-DD HH24:MI:SS'), 9, 2 )  
	                    AND   hour  = SUBSTRING(to_char(get_time_unix(get_unix_time(null)), 'YYYY-MM-DD HH24:MI:SS'), 12, 2 ) 
	                    AND   a.customer_id = b.customer_id
	                    AND   b.cnt > pc_count						;
                            
                               ";

            using var cmd = new NpgsqlCommand();
            cmd.Connection = con;
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();           

            Console.WriteLine($"UPDATE  " + Program.cnt + "  rows   customer_history success!!");
            Program.cnt++;

            con.Close();
            
            return Task.CompletedTask;
        }
    }
}
