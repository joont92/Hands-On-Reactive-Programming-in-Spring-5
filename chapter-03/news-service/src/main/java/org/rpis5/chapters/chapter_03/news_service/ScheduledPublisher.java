package org.rpis5.chapters.chapter_03.news_service;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class ScheduledPublisher<T> implements Publisher<T> {
    final ScheduledExecutorService         scheduledExecutorService;
    final int                              period;
    final TimeUnit                         unit;
    final Callable<? extends Publisher<T>> publisherCallable;

	public ScheduledPublisher(
		Callable<? extends Publisher<T>> publisherCallable,
		int period,
		TimeUnit unit
	) {
		this(publisherCallable, period, unit,
				Executors.newSingleThreadScheduledExecutor());
	}

    public ScheduledPublisher(
	    Callable<? extends Publisher<T>> publisherCallable,
	    int period,
	    TimeUnit unit,
	    ScheduledExecutorService scheduledExecutorService
    ) {
        this.scheduledExecutorService = scheduledExecutorService;
        this.period = period;
	    this.unit = unit;
	    this.publisherCallable = publisherCallable;
    }

	// Subscriber에게 Subscription을 전달하고, 생성자로 받았던 publisher를 scheduling하여 실행
    @Override
    public void subscribe(Subscriber<? super T> actual) {
		SchedulerMainSubscription<T> s = new SchedulerMainSubscription<>(actual, publisherCallable);
	    // SmartMulticastProcessor의 onSubscribe 호출
		// ScheduledPublisher -> SmartMulticastProcessor -> ScheduledPublisher.SchedulerMainSubscription
		// request 횟수 기록?(onNext는 호출하지 않음)
	    actual.onSubscribe(s);

		// 일정 시간마다 run 함수 실행(생성자로 받았던 publisher)
	    s.setScheduledFuture(
            scheduledExecutorService.scheduleWithFixedDelay(s, 0, period, unit)
	    );
    }

    static final class SchedulerMainSubscription<T> implements Subscription, Runnable {

    	final Subscriber<? super T> actual;
	    final Callable<? extends Publisher<T>> publisherCallable;

	    ScheduledFuture<?> scheduledFuture;

	    volatile long requested;
	    static final AtomicLongFieldUpdater<SchedulerMainSubscription> REQUESTED =
			    AtomicLongFieldUpdater.newUpdater(SchedulerMainSubscription.class, "requested");

	    volatile boolean cancelled;

	    SchedulerMainSubscription(
            Subscriber<? super T> actual,
		    Callable<? extends Publisher<T>> publisherCallable
	    ) {
		    this.actual = actual;
		    this.publisherCallable = publisherCallable;
	    }

	    @Override
	    public void request(long n) {
		    if (n <= 0) {
			    onError(new IllegalArgumentException(
					    "Spec. Rule 3.9 - Cannot request a non strictly positive number: " + n
			    ));
			    return;
		    }

			SubscriptionUtils.request(n, this, REQUESTED);
	    }

	    @Override
	    public void cancel() {
		    if (!cancelled) {
			    cancelled = true;

		    	if (scheduledFuture!= null) {
				    scheduledFuture.cancel(true);
			    }
		    }
	    }

	    @Override
	    public void run() {
		    if (!cancelled) {
			    try {
					// NewsPreparationOperator가 DBPublisher로 부터 뉴스레터를 받은 뒤, 집계가 되면 SchedulerInnerSubscriber 호출하고, SchedulerInnerSubscriber는 SmartMulticastProcessor 호출
				    Publisher<T> innerPublisher = Objects.requireNonNull(publisherCallable.call()); // NewsPreperationOperator
				    if (requested == Long.MAX_VALUE) {
						// 전달받은 SmartMulticastProcessor가 아닌 SchedulerInnerSubscriber를 NewsPreparationOperator에 전달
						// NewsPreparationOperator에서 NewsPreparationInner subscriber 추가해서 DBPublisher에 전달
						// DBPublisher에서 NewsPreparationInner의 onSubscriber 호출

						// 1. DBPublisher -> NewsPreparationInner.onSubscribe -> SchedulerInnerSubscriber.onSubscribe
						// 2. SchedulerInnerSubscriber에서 NewsPreparationInner.request 호출 -> queue에 남아있을 경우 SchedulerInnerSubscriber.onNext 호출
						// 3. NewsPreparationInner.onSubscribe에서 dbPublisher.request 호출 -> NewsPreparationInner.onNext 호출: queue에 쌓음
						// 4. queue에 데이터가 다 쌓였을 경우 SchedulerInnerSubscriber.onNext 호출 -> SmartMulticastProcessor.onNext 호출
					    innerPublisher.subscribe(new FastSchedulerInnerSubscriber<>(this));
				    }
				    else {
					    innerPublisher.subscribe(new SlowSchedulerInnerSubscriber<>(this));
				    }
			    }
			    catch (Exception e) {
				    onError(e);
			    }
		    }
	    }

	    void onError(Throwable throwable) {
	    	if (cancelled) {
	    		return;
		    }

			cancel();
			actual.onError(throwable);
	    }

	    void tryEmit(T e) {
		    for (;;) {
			    long r = requested;

			    if (r <= 0) {
				    onError(new IllegalStateException("Lack of demand"));
				    return;
			    }

			    if (requested == Long.MAX_VALUE ||
					    REQUESTED.compareAndSet(this, r, r - 1)) {
				    emit(e);
				    return;
			    }
		    }
	    }

	    void emit(T e) {
		    Subscriber<? super T> a = this.actual;

		    a.onNext(e); // SmartMulticastProcessor의 onNext 호출
	    }


	    void setScheduledFuture(ScheduledFuture<?> scheduledFuture) {
		    this.scheduledFuture = scheduledFuture;
	    }
    }

    static abstract class SchedulerInnerSubscriber<T> implements Subscriber<T> {

    	final SchedulerMainSubscription<T> parent;

    	Subscription s;

	    SchedulerInnerSubscriber(SchedulerMainSubscription<T> parent) {
		    this.parent = parent;
	    }

	    @Override
	    public void onSubscribe(Subscription s) {
	    	if (this.s == null) {
			    this.s = s;
			    s.request(Long.MAX_VALUE); // NewsPreparationInner.request 호출
		    } else {
	    		s.cancel();
		    }
	    }

	    @Override
	    public void onError(Throwable t) {
			parent.onError(t);
	    }

	    @Override
	    public void onComplete() {

	    }
    }

    static final class FastSchedulerInnerSubscriber<T> extends SchedulerInnerSubscriber<T> {

	    FastSchedulerInnerSubscriber(SchedulerMainSubscription<T> parent) {
		    super(parent);
	    }

	    @Override
	    public void onNext(T t) {
	    	if (parent.cancelled) {
	    		s.cancel();
	    		return;
		    }

			// SchedulerMainSubscription.parent -> SchedulerMainSubscription.request
		    parent.emit(t);
	    }
    }

    static final class SlowSchedulerInnerSubscriber<T> extends SchedulerInnerSubscriber<T> {

	    SlowSchedulerInnerSubscriber(SchedulerMainSubscription<T> parent) {
			super(parent);
		}

		@Override
		public void onNext(T t) {
			if (parent.cancelled) {
				s.cancel();
				return;
			}

			parent.tryEmit(t);
		}
	}
}
