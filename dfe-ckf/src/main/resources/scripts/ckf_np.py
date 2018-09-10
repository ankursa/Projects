import numpy as np

def callCKF(params,*args):
  data = args[0]
  observations = args[1]
  pvars  = args[2]
  mode   = args[3]

  stateSize = len(pvars)

  state = params[:stateSize]
  proc_noise = np.diag([item**2 for item in params[stateSize:2*stateSize]])
  meas_noise = np.array([[item**2 for item in params[2*stateSize:]]])

  return ckf(state,*(data,observations,pvars,proc_noise,meas_noise,mode))

def ckf(beta,*args):

  state = np.array([[item] for item in beta])
  data = args[0]
  observations = args[1]
  pvars  = args[2]
  proc_noise = args[3]
  meas_noise = args[4]
 
  mode = args[5]

  state_size = len(state)

  epsilon_0 = np.diag([np.sqrt(state_size) for i in range(state_size)])
  epsilon_1 = np.diag([-np.sqrt(state_size) for i in range(state_size)])

  epsilon = np.concatenate((epsilon_0,epsilon_1),axis=1)

  timesteps = data.shape[0]  

  likelihood_sum = 0
  state_list = []
  vars_list  = []

  for i in range(timesteps):
     #Time Update
     #cholesky decomp #38.
     p_chol = np.sqrt(pvars)

     #cubature points calculations #39
     cubature_points = np.dot(p_chol,epsilon) + state

     #propagated cubature points #40
     propagated_cub_points = cubature_points

     #predicted state #41
     predicted_state = np.array([np.mean(propagated_cub_points,axis=1)]).T
      
     #predicted covariance #42
     predicted_err_covariance = (np.dot(propagated_cub_points,propagated_cub_points.T)/(2*state_size)) - np.dot(predicted_state,predicted_state.T) + proc_noise

     predicted_err_covariance = np.diag(np.diag(predicted_err_covariance))

     #Measurement Updates
     #Factorize 43
     pred_err_cov_chol = np.sqrt(predicted_err_covariance)

     #Evaluating cubature points #44
     cubature_points_new = np.dot(pred_err_cov_chol,epsilon) + predicted_state

     #evaluate propagated cubature points (propagated measurements)#45
     propagated_measurements = np.dot(data[i],cubature_points_new)

     #predicted measurement #46
     predicted_measurement = np.mean(propagated_measurements,axis=0)

     #innovation covariance matrix #47
     innovation_cov_matrix = (np.dot(propagated_measurements,propagated_measurements.T)/(2*state_size))-np.dot(predicted_measurement,predicted_measurement.T) + meas_noise 

     #cross covariance matrix #48
     cross_cov_matrix = np.array([np.dot(cubature_points_new,propagated_measurements.T)/(2*state_size)]).T - np.dot(predicted_state,predicted_measurement.T)

     #kalman gain #49
     kalman_gain = np.dot(cross_cov_matrix,np.linalg.inv(innovation_cov_matrix))

     #updated state #50
     state = predicted_state + np.dot(kalman_gain,np.array([(observations[i]-predicted_measurement)]))
     #updated error covariance #51
     pvars = predicted_err_covariance - np.dot(kalman_gain,np.dot(innovation_cov_matrix,kalman_gain.T))
     pvars = np.diag(np.diag(pvars))

     error = np.array([observations[i]-predicted_measurement])
     likelihood_sum += np.log(innovation_cov_matrix.trace()) + np.dot(error.T,np.dot(np.linalg.inv(innovation_cov_matrix),error))

     state_list.append(state)
     vars_list.append(pvars)

  likelihood_sum = likelihood_sum/2

  #print likelihood_sum
  if mode == 0:
     return likelihood_sum 
  else:
     return state_list,vars_list,likelihood_sum

