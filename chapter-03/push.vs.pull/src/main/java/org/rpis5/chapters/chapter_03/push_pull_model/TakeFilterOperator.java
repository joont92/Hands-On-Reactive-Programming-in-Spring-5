package org.rpis5.chapters.chapter_03.push_pull_model;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.Predicate;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class TakeFilterOperator<T> implements Publisher<T> {

	private final Publisher<T> source;
	private final int          take;
	private final Predicate<T> predicate;

	public TakeFilterOperator(Publisher<T> source, int take, Predicate<T> predicate) {
		this.source = source;
		this.take = take;
		this.predicate = predicate;
	}

	public void subscribe(Subscriber s) {
		// upstream(AsyncDatabaseClient)의 subscribe 호출
		// upstream의 subscribe에서는 TaskFilterInner의 onSubscribe를 호출
		source.subscribe(new TakeFilterInner<>(s, take, predicate));
	}

	static final class TakeFilterInner<T> implements Subscriber<T>, Subscription {

		final Subscriber<T> actual;
		final int           take;
		final Predicate<T>  predicate;
		final Queue<T>      queue;

		Subscription current;
		int          remaining;
		int          filtered;
		Throwable    throwable;
		boolean      done;

		volatile long requested;
		static final AtomicLongFieldUpdater<TakeFilterInner> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(TakeFilterInner.class, "requested");

		volatile int wip;
		static final AtomicIntegerFieldUpdater<TakeFilterInner> WIP =
				AtomicIntegerFieldUpdater.newUpdater(TakeFilterInner.class, "wip");

		TakeFilterInner(Subscriber<T> actual, int take, Predicate<T> predicate) {
			this.actual = actual;
			this.take = take;
			this.remaining = take;
			this.predicate = predicate;
			this.queue = new ConcurrentLinkedQueue<>();
		}

		// publisher(upstream)에서 onSubscribe를 호출하거나 subscriber가 downstream onSubscribe를 호출하거나
		// TakeFilterInner subscriber를 TakeFilterOperator.source.subscribe에 던졌기 때문에
		// upstream(AsyncDatabaseClient.subscribe)에서 이 메서드 호출
		public void onSubscribe(Subscription current) { // publisher(AsyncDatabaseClient)에서 전달한 Subscription
			if (this.current == null) {
				this.current = current;

				// downstream(PullerTest)의 onSubscribe 호출
				// -> TakeFilterInner의 request 호출(queue에 남아있는 데이터 전달)
				this.actual.onSubscribe(this);
				if (take > 0) {
					// upstream(AsyncDatabaseClient에서 생성한 Subscription) 호출(db에서 새로운 데이터 가져옴)
					this.current.request(take); // 이후 80번 라인의 onNext 호출(db에서 가져온 데이터를 다시 downstream에 전달)
				} else {
					onComplete();
				}
			}
			else {
				current.cancel();
			}
		}

		public void onNext(T element) { // upstream(AsyncDatabaseClient 에서 생성한 Subscription)의 request에서 호출
			if (done) {
				return;
			}

			long r = requested;
			Subscriber<T> a = actual;
			Subscription s = current;

			// remaining 깎아가면서 조건에 충족하는 요소만 downstream(PullerTest)으로 전달
			if (remaining > 0) {
				boolean isValid = predicate.test(element);
				boolean isEmpty = queue.isEmpty();

				if (isValid && r > 0 && isEmpty) {
					a.onNext(element); // downstream(PullerTest)의 onNext 호출
					remaining--;

					REQUESTED.decrementAndGet(this);
					if (remaining == 0) {
						s.cancel(); // upstream(AsyncDatabaseClient.Subscription)의 cancel 호출
						onComplete();
					}
				}
				else if (isValid && (r == 0 || !isEmpty)) {
					queue.offer(element);
					remaining--;

					if (remaining == 0) {
						s.cancel(); // upstream(AsyncDatabaseClient.Subscription)의 cancel 호출
						onComplete(); // 완료되면 downstream(PullerTest)의 onComplete 호출
					}
					drain(a, r);
				}
				else if (!isValid) {
					filtered++;
				}
			}
			else {
				s.cancel();
				onComplete();
			}

			// 요청한(70라인) 요소의 반 정도 소비했을 때 upstream(AsyncDatabaseClient)에 추가 데이터 요청
			if (filtered > 0 && remaining / filtered < 2) {
				s.request(take); // upstream(AsyncDatabaseClient.Subscription)의 request 호출
				filtered = 0;
			}
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				return;
			}

			done = true;

			if (queue.isEmpty()) {
				actual.onError(t); // downstream(PullerTest)의 onError 호출
			}
			else {
				throwable = t;
			}
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}

			done = true;

			if (queue.isEmpty()) {
				actual.onComplete(); // downstream(PullerTest)의 onComplete 호출
			}
		}

		@Override
		public void request(long n) {
			if (n <= 0) {
				onError(new IllegalArgumentException(
					"Spec. Rule 3.9 - Cannot request a non strictly positive number: " + n
				));
			}

			drain(actual, SubscriptionUtils.request(n, this, REQUESTED));
		}

		@Override
		public void cancel() {
			if (!done) {
				current.cancel();
			}

			queue.clear();
		}

		// queue에 남아있으면 downstream(PullerTest)에 전달
		void drain(Subscriber<T> a, long r) {
			if (queue.isEmpty() || r == 0) {
				return;
			}

			int wip;

			if ((wip = WIP.incrementAndGet(this)) > 1) {
				return;
			}

			int c = 0;
			boolean empty;

			for (;;) {
				T e;
				while (c != r && (e = queue.poll()) != null) {
					a.onNext(e); // downstream(PullerTest)의 onNext 호출
					c++;
				}


				empty = queue.isEmpty();
				r = REQUESTED.addAndGet(this, -c);
				c = 0;

				if (r == 0 || empty) {
					if (done && empty) {
						if (throwable == null) {
							a.onComplete();
						}
						else {
							a.onError(throwable);
						}
						return;
					}

					wip = WIP.addAndGet(this, -wip);

					if (wip == 0) {
						return;
					}
				}
				else {
					wip = this.wip;
				}
			}
		}
	}
}                                                                  
