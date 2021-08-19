package org.rpis5.chapters.chapter_03.news_service;

import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.rpis5.chapters.chapter_03.news_service.dto.NewsLetter;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

// (NewsServicePublisher에서 얘를 사용하지 않음)
// 이런식으로 Subscriber를 사용하면
// 1. newsletter를 원하는 만큼 요청하여 queue에 담고,
// 2. 사용자가 요청할 경우 queue에서 읽어서 보여주고,
// 3. queue에 없을 경우 다시 newsletter를 요청
// 하는 기능을 제공해줄 수 있다
public class NewsServiceSubscriber implements Subscriber<NewsLetter> {
    // NewsServiceSubscriber 객체를 생성해서 쓰는데 Concurrent 관련 라이브러리를 사용한 이유는?
    final Queue<NewsLetter> mailbox   = new ConcurrentLinkedQueue<>();
    final AtomicInteger     remaining = new AtomicInteger();
    final int               take;

    Subscription subscription;

    public NewsServiceSubscriber(int take) {
        this.take = take;
        this.remaining.set(take);
    }

    public void onSubscribe(Subscription s) {
        if (subscription == null) {
            subscription = s;
            subscription.request(take);
        }
        else {
            s.cancel();
        }
    }

    public void onNext(NewsLetter newsLetter) {
        Objects.requireNonNull(newsLetter);

        mailbox.offer(newsLetter); // 요청한 newsLetter를 받아 mailbox queue에 저장해둠
    }

    public void onError(Throwable t) {
        Objects.requireNonNull(t);

        if (t instanceof ResubscribableErrorLettter) {
            subscription = null;
            ((ResubscribableErrorLettter) t).resubscribe(this);
        }
    }

    public void onComplete() {
        subscription = null;
    }

    public Optional<NewsLetter> eventuallyReadDigest() {
        NewsLetter letter = mailbox.poll(); // mailbox queue에 저장된 뉴스레터를 읽음
        if (letter != null) {
            if (remaining.decrementAndGet() == 0) { // 다 읽었을 경우 뉴스레터 요청
                subscription.request(take);
                remaining.set(take);
            }
            return Optional.of(letter);
        } return Optional.empty();
    }
}          
