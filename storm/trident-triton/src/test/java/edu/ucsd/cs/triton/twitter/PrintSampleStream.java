package edu.ucsd.cs.triton.twitter;

import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.StallWarning;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

public final class PrintSampleStream {
  /**
   * Main entry of this application.
   *
   * @param args
   */
  public static void main(String[] args) throws TwitterException {
      TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
      StatusListener listener = new StatusListener() {
          @Override
          public void onStatus(Status status) {
              System.out.println("@" + status.getUser().getScreenName() + " - " + status.getText());
          }

          @Override
          public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
              System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
          }

          @Override
          public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
              System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
          }

          @Override
          public void onScrubGeo(long userId, long upToStatusId) {
              System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
          }

          @Override
          public void onStallWarning(StallWarning warning) {
              System.out.println("Got stall warning:" + warning);
          }

          @Override
          public void onException(Exception ex) {
              ex.printStackTrace();
          }
      };
      twitterStream.addListener(listener);
      twitterStream.sample();
  }
}
