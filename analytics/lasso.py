from sklearn import linear_model

def Lasso(mat_builder, vec):
    lasso = linear_model.Lasso(fit_intercept=False)
    lasso.fit(mat_builder.CSR(), vec)
    result = {}
    for i, val in enumerate(lasso.coef_):
        result[mat_builder.GetColKey(i)] = val
    return result
