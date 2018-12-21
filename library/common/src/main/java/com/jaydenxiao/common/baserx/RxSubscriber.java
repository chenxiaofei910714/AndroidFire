package com.jaydenxiao.common.baserx;

import android.app.Activity;
import android.content.Context;

import com.jaydenxiao.common.baseapp.BaseApplication;
import com.jaydenxiao.common.R;
import com.jaydenxiao.common.commonutils.NetWorkUtils;
import com.jaydenxiao.common.commonwidget.LoadingDialog;

import io.reactivex.Observable;
import io.reactivex.Observer;
import io.reactivex.disposables.Disposable;
import io.reactivex.disposables.Disposables;


/**
 * des:订阅封装
 * Created by xsf
 * on 2016.09.10:16
 */
/********************使用例子********************/
/*_apiService.login(mobile, verifyCode)
        .//省略
        .subscribe(new RxSubscriber<User user>(mContext,false) {
@Override
public void _onNext(User user) {
        // 处理user
        }

@Override
public void _onError(String msg) {
        ToastUtil.showShort(mActivity, msg);
        });*/
public abstract class RxSubscriber<T> implements Observer<T> {

    private Context mContext;
    private String msg;
    private boolean showDialog=true;
    private RxManager mRxManager;

    /**
     * 是否显示浮动dialog
     */
    public void showDialog() {
        this.showDialog= true;
    }
    public void hideDialog() {
        this.showDialog= true;
    }

    public RxSubscriber(Context context, String msg,boolean showDialog,RxManager mRxManager) {
        this.mContext = context;
        this.msg = msg;
        this.showDialog=showDialog;
        this.mRxManager=mRxManager;
    }
    public RxSubscriber(Context context,RxManager mRxManager) {
        this(context, BaseApplication.getAppContext().getString(R.string.loading),true,mRxManager);
    }
    public RxSubscriber(Context context,boolean showDialog,RxManager mRxManager) {
        this(context, BaseApplication.getAppContext().getString(R.string.loading),showDialog,mRxManager);
    }

    @Override
    public void onComplete() {
        if (showDialog)
            LoadingDialog.cancelDialogForLoading();
    }

    @Override
    public void onSubscribe(Disposable d) {
        if (showDialog) {
            try {
                LoadingDialog.showDialogForLoading((Activity) mContext,msg,true);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        mRxManager.add(d);
    }


    @Override
    public void onNext(T t) {
        _onNext(t);
    }
    @Override
    public void onError(Throwable e) {
        if (showDialog)
            LoadingDialog.cancelDialogForLoading();
        e.printStackTrace();
        //网络
        if (!NetWorkUtils.isNetConnected(BaseApplication.getAppContext())) {
            _onError(BaseApplication.getAppContext().getString(R.string.no_net));
        }
        //服务器
        else if (e instanceof ServerException) {
            _onError(e.getMessage());
        }
        //其它
        else {
            _onError(BaseApplication.getAppContext().getString(R.string.net_error));
        }
    }

    protected abstract void _onNext(T t);

    protected abstract void _onError(String message);

}
