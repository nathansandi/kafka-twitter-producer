package com.twitter.producer.config

import CONSUMER_KEYS
import CONSUMER_SECRETS
import SECRET
import TOKEN
import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.Client
import com.twitter.hbc.core.Constants
import com.twitter.hbc.core.Hosts
import com.twitter.hbc.core.HttpHosts
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import com.twitter.hbc.httpclient.auth.Authentication
import com.twitter.hbc.httpclient.auth.OAuth1
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue

@Component
class TwitterConfig {
    companion object {
        fun createTwitterClient(msgQueue: BlockingQueue<String>, trackTerms: List<String>): Client? {
            /** Setting up a connection    */
            val hosebirdHosts: Hosts = HttpHosts(Constants.STREAM_HOST)
            val hbEndpoint = StatusesFilterEndpoint()
            // Term that I want to search on Twitter
            hbEndpoint.trackTerms(trackTerms)

            // Twitter API and tokens
            val hosebirdAuth: Authentication =
                OAuth1(CONSUMER_KEYS, CONSUMER_SECRETS, TOKEN, SECRET)

            /** Creating a client    */
            val builder = ClientBuilder()
                .name("Hosebird-Client")
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hbEndpoint)
                .processor(StringDelimitedProcessor(msgQueue))
            return builder.build()
        }
    }
}