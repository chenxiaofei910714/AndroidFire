package com.jaydenxiao.common.baserx;

import com.jaydenxiao.common.basebean.BaseRespose;
import com.jaydenxiao.common.commonutils.LogUtils;

import io.reactivex.Observable;
import io.reactivex.ObservableEmitter;
import io.reactivex.ObservableOnSubscribe;
import io.reactivex.ObservableSource;
import io.reactivex.ObservableTransformer;
import io.reactivex.android.schedulers.AndroidSchedulers;
import io.reactivex.functions.Function;
import io.reactivex.schedulers.Schedulers;

/**
 * des:对服务器返回数据成功和失败处理
 * Created by xsf
 * on 2016.09.9:59
 */
/**************使用例子******************/
/*_apiService.login(mobile, verifyCode)
        .compose(RxSchedulersHelper.io_main())
        .compose(RxResultHelper.handleResult())
        .//省略*/

public class RxHelper {
    /**
     * 对服务器返回数据进行预处理
     *
     * @param <T>
     * @return
     */
    public static <T> ObservableTransformer<BaseRespose<T>, T> handleResult() {
        return new ObservableTransformer<BaseRespose<T>, T>() {
            @Override
            public ObservableSource<T> apply(Observable<BaseRespose<T>> upstream) {
                return upstream.flatMap(new Function<BaseRespose<T>, ObservableSource<T>>() {
                    @Override
                    public ObservableSource<T> apply(BaseRespose<T> tBaseRespose) throws Exception {
                        LogUtils.logd("result from api : " + tBaseRespose);
                        if (tBaseRespose.success()) {
                            return createData(tBaseRespose.data);
                        } else {
                            return Observable.error(new ServerException(tBaseRespose.msg));
                        }
                    }


                }).subscribeOn(Schedulers.io()).observeOn(AndroidSchedulers.mainThread());
            }
        };

    }

    /**
     * 创建成功的数据
     *
     * @param data
     * @param <T>
     * @return
     */
    private static <T> Observable<T> createData(final T data) {
        return Observable.create(new ObservableOnSubscribe<T>() {

            @Override
            public void subscribe(ObservableEmitter<T> emitter) throws Exception {
                try {
                    emitter.onNext(data);
                    emitter.onComplete();
                } catch (Exception e) {
                    emitter.onError(e);
                }
            }
        });

    }
}
