--run this before kicking off the full pipeline
create table final_sim_results
(
  newsid     bigint,
  sim_tweets text,
  news_date  text,
  tweet_file text
);