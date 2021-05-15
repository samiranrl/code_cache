# K-fold cross-validation

from sklearn.model_selection import KFold 


X = df[[i for i in df.columns if 'feature' in i]]
y = df['target']


kf = KFold(n_splits=5)

for train_index, test_index in kf.split(X):
    X_train, X_test = X.loc[train_index], X.loc[test_index]
    y_train, y_test = y.loc[train_index], y.loc[test_index]
    
