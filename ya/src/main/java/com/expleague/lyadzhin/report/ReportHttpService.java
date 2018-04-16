package com.expleague.lyadzhin.report;

import com.expleague.lyadzhin.report.bridge.CommunicationBridgeDomain;
import com.expleague.lyadzhin.report.cfg.Configuration;
import com.expleague.lyadzhin.report.cfg.ConfigurationDomain;
import com.expleague.lyadzhin.report.http.HttpRequest;
import com.expleague.lyadzhin.report.http.HttpResponse;
import com.expleague.lyadzhin.report.http.UserHttpCommunicationDomain;
import com.expleague.lyadzhin.report.viewports.ViewportsDomain;
import com.expleague.yasm4u.JobExecutorService;
import com.expleague.yasm4u.domains.mr.env.LocalMREnv;
import com.expleague.yasm4u.domains.wb.Whiteboard;
import com.expleague.yasm4u.impl.MainThreadJES;
import com.expleague.lyadzhin.report.sources.SourceCommunicationDomain;
import com.expleague.yasm4u.domains.wb.impl.WhiteboardImpl;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * User: lyadzhin
 * Date: 04.04.15 10:23
 */
public class ReportHttpService {
  public static void main(String[] args) {
    final HttpRequest httpRequest = new MyHttpRequest();
    final HttpResponse httpResponse = new MyHttpResponse();

    final Whiteboard whiteboard = new WhiteboardImpl(new LocalMREnv(), "main");
    final UserHttpCommunicationDomain userCommunicationDomain = new UserHttpCommunicationDomain(httpRequest, httpResponse, whiteboard);
    final SourceCommunicationDomain sourceCommunicationDomain = new SourceCommunicationDomain();

    final Configuration configuration;
    try {
      final ConfigurationDomain configurationDomain = new ConfigurationDomain();
      final JobExecutorService jes = new MainThreadJES(false,
              userCommunicationDomain,
              whiteboard,
              sourceCommunicationDomain,
              configurationDomain
      );
      final Future<Configuration> configurationFuture = jes.calculate(ConfigurationDomain.REF_CONFIGURATION);
      configuration = configurationFuture.get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }

    final UserHttpCommunicationDomain.CommunicationStatus communicationStatus;
    try {
      final ViewportsDomain viewportsDomain = new ViewportsDomain(configuration);
      final CommunicationBridgeDomain communicationBridgeDomain = new CommunicationBridgeDomain(configuration, userCommunicationDomain, viewportsDomain, whiteboard);
      final JobExecutorService jes = new MainThreadJES(true,
              sourceCommunicationDomain,
              userCommunicationDomain,
              whiteboard,
              viewportsDomain,
              communicationBridgeDomain
      );
      final Future<UserHttpCommunicationDomain.CommunicationStatus> statusFuture = jes.calculate(userCommunicationDomain.goal());
      communicationStatus = statusFuture.get();
    } catch (InterruptedException|ExecutionException e) {
      throw new RuntimeException(e);
    }

    System.out.println(communicationStatus);
  }

  private static class MyHttpRequest implements HttpRequest {
  }

  private static class MyHttpResponse implements HttpResponse {
  }
}
