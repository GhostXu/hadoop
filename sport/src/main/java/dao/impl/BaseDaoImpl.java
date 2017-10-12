package dao.impl;

import dao.BaseDao;
import org.apache.ibatis.session.SqlSessionFactory;

import javax.annotation.Resource;
import java.lang.reflect.ParameterizedType;
import java.util.List;
import java.util.Map;

public abstract class BaseDaoImpl<T> implements BaseDao<T> {

    @Resource(name="sqlSessionFactory")
    private SqlSessionFactory sqlSessionFactory;

    public Class<T> classz;

    private String objName;

    public String getObjName() {
        return objName;
    }

    public void setObjName(String objName) {
        this.objName = objName;
    }

    public BaseDaoImpl(){
        ParameterizedType type = (ParameterizedType) this.getClass().getGenericSuperclass();
        classz = (Class<T>) type.getActualTypeArguments()[0];

        objName = classz.getSimpleName();
    }

    @Override
    public T find(Map<String,Object> map) {
        T t = sqlSessionFactory.openSession().selectOne("",map);

        return t;
    }

    public List<T> findList(){
        return sqlSessionFactory.openSession().selectList("baseSql.selectList",objName);
    }
    @Override
    public void update(Object param) {

    }

    @Override
    public void insert() {

    }

    @Override
    public void delete(Object param) {

    }
}
