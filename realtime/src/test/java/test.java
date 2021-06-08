import com.alibaba.fastjson.JSONObject;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.beanutils.BeanUtilsBean;

import java.lang.reflect.InvocationTargetException;

/**
 * @author: yigang
 * @date: 2021/3/31
 * @desc:
 */
public class test {
    public static void main(String[] args) throws Exception {
        JSONObject a = new JSONObject();


//        BeanUtils.setProperty(a,"id",11);

//        a.put("age","13");
        fun(JSONObject.class);
//        System.out.println(a.toJSONString());
    }
    public static <T> void fun(Class<T> clazz) throws Exception {
        T data = clazz.newInstance();
        BeanUtils.setProperty(data,"id",11);
        System.out.println(data.toString());
    }
}
